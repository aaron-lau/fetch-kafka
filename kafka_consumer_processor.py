from confluent_kafka import Consumer, Producer
import json
from collections import defaultdict
import time
import hashlib
import base64

# Kafka configuration
BOOTSTRAP_SERVERS = 'localhost:29092'
INPUT_TOPIC = 'user-login'
OUTPUT_TOPIC = 'processed-logins'

# PostgreSQL configuration
PG_CONFIG = {
    'dbname': 'your_database',
    'user': 'your_user',
    'password': 'your_password',
    'host': 'localhost',
    'port': '5432'
}

# Create table SQL
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS user_logins (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255),
    app_version VARCHAR(50),
    device_type VARCHAR(50),
    locale VARCHAR(50),
    device_id VARCHAR(255),
    ip_address VARCHAR(255),
    processed_timestamp BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# Insert SQL
INSERT_SQL = """
INSERT INTO user_logins (
    user_id, app_version, device_type, locale, 
    device_id, ip_address, processed_timestamp
) VALUES (
    %s, %s, %s, %s, %s, %s, %s
);
"""

def init_postgres():
    """Initialize PostgreSQL connection and create table if not exists"""
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    cur.close()
    conn.autocommit = False
    return conn

# Create Kafka consumer
consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'login-processor',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([INPUT_TOPIC])

# Create Kafka producer
producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

def mask_sensitive_field(value, prefix):
    """
    Masks sensitive data while maintaining uniqueness for analysis.
    Returns a deterministic hash with a prefix for easy identification.
    """
    if not value or value == 'unknown':
        return f"{prefix}_unknown"
    
    # Create a deterministic hash of the value
    hash_object = hashlib.sha256(str(value).encode())
    # Take first 16 characters of base64 encoded hash for readability
    hashed_value = base64.b64encode(hash_object.digest())[:16].decode()
    return f"{prefix}_{hashed_value}"

# Data processing function
def process_message(message):
    try:
        # Extract relevant fields with default values
        user_id = message.get('user_id', 'unknown')
        app_version = message.get('app_version', 'unknown')
        device_type = message.get('device_type', 'unknown')
        locale = message.get('locale', 'unknown')
        device_id = message.get('device_id', 'unknown')
        ip_address = message.get('ip', 'unknown')
        timestamp = int(message.get('timestamp', time.time()))

        # Mask sensitive fields
        masked_device_id = mask_sensitive_field(device_id, 'dev')
        masked_ip = mask_sensitive_field(ip_address, 'ip')

        # Store original hash mapping for debugging (in production, you might want to log this separately)
        # if device_id != 'unknown':
        #     print(f"Device ID mapping - Original: {device_id} -> Masked: {masked_device_id}")
        # if ip_address != 'unknown':
        #     print(f"IP mapping - Original: {ip_address} -> Masked: {masked_ip}")

        # Perform some basic processing
        processed_data = {
            'user_id': user_id,
            'app_version': app_version,
            'device_type': device_type,
            'locale': locale,
            'device_id': masked_device_id,
            'ip_address': masked_ip,
            'processed_timestamp': int(time.time())
        }

        return processed_data
    except Exception as e:
        print(f"Error in process_message: {e}")
        print(f"Problematic message: {message}")
        return None

# Aggregation data structures
login_counts = defaultdict(int)
device_type_counts = defaultdict(int)
locale_counts = defaultdict(int)
masked_ip_counts = defaultdict(int)
masked_device_counts = defaultdict(int)

# Initialize PostgreSQL connection
pg_conn = init_postgres()
pg_cur = pg_conn.cursor()

# Buffer for batch inserts
insert_buffer = []
BATCH_SIZE = 100

# Main processing loop
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            # Process the message
            message_value = json.loads(msg.value().decode('utf-8'))
            processed_data = process_message(message_value)
            
            if processed_data is None:
                continue  # Skip this message and move to the next one

            # Update aggregations
            login_counts[processed_data['user_id']] += 1
            device_type_counts[processed_data['device_type']] += 1
            locale_counts[processed_data['locale']] += 1
            masked_ip_counts[processed_data['ip_address']] += 1
            masked_device_counts[processed_data['device_id']] += 1

            # Prepare data for PostgreSQL
            pg_record = (
                processed_data['user_id'],
                processed_data['app_version'],
                processed_data['device_type'],
                processed_data['locale'],
                processed_data['device_id'],
                processed_data['ip_address'],
                processed_data['processed_timestamp']
            )
            insert_buffer.append(pg_record)

            # Batch insert to PostgreSQL when buffer is full
            if len(insert_buffer) >= BATCH_SIZE:
                execute_batch(pg_cur, INSERT_SQL, insert_buffer)
                pg_conn.commit()
                insert_buffer = []

            # Produce processed data to output topic
            producer.produce(OUTPUT_TOPIC, json.dumps(processed_data).encode('utf-8'))
            producer.flush()

            # Print some insights (you can adjust the frequency as needed)
            if sum(login_counts.values()) % 100 == 0:
                print("\n=== Processing Insights ===")
                print("Total logins processed:", sum(login_counts.values()))
                print("Top 3 device types:", sorted(device_type_counts.items(), key=lambda x: x[1], reverse=True)[:3])
                print("Top 3 locales:", sorted(locale_counts.items(), key=lambda x: x[1], reverse=True)[:3])

                # Report masked IPs with high login counts
                frequent_ips = {ip: count for ip, count in masked_ip_counts.items() if count > 10}
                if frequent_ips:
                    print("\nMasked IPs with high login counts (>10):", frequent_ips)

                # Report devices with high login counts
                frequent_devices = {dev: count for dev, count in masked_device_counts.items() if count > 5}
                if frequent_devices:
                    print("\nMasked devices with high login counts (>5):", frequent_devices)

        except Exception as e:
            print(f"Error processing message: {e}")
            print(f"Problematic message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    pass

finally:

    # Insert any remaining records in the buffer
    if insert_buffer:
        execute_batch(pg_cur, INSERT_SQL, insert_buffer)
        pg_conn.commit()
    
    # Close all connections
    pg_cur.close()
    pg_conn.close()
    consumer.close()