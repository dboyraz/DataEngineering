import json
import sys
from pathlib import Path

# Maintain compatibility with your folder structure
sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer

# 1. Configuration
server = 'localhost:9092'
topic_name = 'green-trips'

# 2. Initialize the Consumer
# auto_offset_reset='earliest' ensures we read from the beginning of the topic
# consumer_timeout_ms=5000 stops the loop if no new data arrives for 5 seconds
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='green-trips-counter-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=5000 
)

print(f"Counting long trips in {topic_name} (trip_distance > 5.0)...")

long_trip_count = 0
total_messages = 0

try:
    for message in consumer:
        trip_data = message.value
        total_messages += 1
        
        # Check if the trip distance is greater than 5.0
        if trip_data.get('trip_distance', 0) > 5.0:
            long_trip_count += 1
            
        # Optional: print progress every 10,000 messages
        if total_messages % 10000 == 0:
            print(f"Processed {total_messages} messages...")

except Exception as e:
    print(f"Error while consuming: {e}")

finally:
    consumer.close()

# 3. Output the result
print("\n" + "="*30)
print(f"Total messages processed: {total_messages}")
print(f"Trips with distance > 5.0: {long_trip_count}")
print("="*30)