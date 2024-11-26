from confluent_kafka import Consumer, Producer
import json
from collections import defaultdict
import time

# Kafka configuration
BOOTSTRAP_SERVERS = 'localhost:29092'
INPUT_TOPIC = 'user-login'
OUTPUT_TOPIC = 'processed-logins'

# Create Kafka consumer
consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'login-processor',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([INPUT_TOPIC])

# Create Kafka producer
producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

# Data processing function
def process_message(message):
    try:
        # Extract relevant fields with default values
        user_id = message.get('user_id', 'unknown')
        app_version = message.get('app_version', 'unknown')
        device_type = message.get('device_type', 'unknown')
        locale = message.get('locale', 'unknown')
        device_id = message.get('device_id', 'unknown')
        timestamp = int(message.get('timestamp', time.time()))

        # Perform some basic processing
        processed_data = {
            'user_id': user_id,
            'app_version': app_version,
            'device_type': device_type,
            'locale': locale,
            'device_id': device_id,
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

            # Produce processed data to output topic
            producer.produce(OUTPUT_TOPIC, json.dumps(processed_data).encode('utf-8'))
            producer.flush()

            # Print some insights (you can adjust the frequency as needed)
            if sum(login_counts.values()) % 100 == 0:
                print("Total logins processed:", sum(login_counts.values()))
                print("Top 3 device types:", sorted(device_type_counts.items(), key=lambda x: x[1], reverse=True)[:3])
                print("Top 3 locales:", sorted(locale_counts.items(), key=lambda x: x[1], reverse=True)[:3])

        except Exception as e:
            print(f"Error processing message: {e}")
            print(f"Problematic message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    pass

finally:
    consumer.close()