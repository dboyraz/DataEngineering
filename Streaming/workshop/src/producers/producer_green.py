import json
import sys
from time import time
from pathlib import Path

# Ensures we can import from the parent directory if needed
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer

# 1. Configuration
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
topic_name = 'green-trips'
server = 'localhost:9092'

# 2. Define the specific columns to keep
columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount'
]

# 3. Load the data
print(f"Downloading entire dataset from {url}...")
df = pd.read_parquet(url, columns=columns).head(1000)
print(f"Loaded {len(df)} rows.")

# 4. Initialize the Kafka Producer
# We use a simple lambda to serialize the dict to JSON bytes
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 5. Measure sending time
print(f"Starting to send data to {topic_name}...")
t0 = time()

for row in df.itertuples(index=False):
    # Convert row to dictionary
    row_dict = row._asdict()
    
    # Convert datetime objects to strings
    row_dict['lpep_pickup_datetime'] = str(row_dict['lpep_pickup_datetime'])
    row_dict['lpep_dropoff_datetime'] = str(row_dict['lpep_dropoff_datetime'])
    
    # Send to topic
    producer.send(topic_name, value=row_dict)

# Important: ensure all messages are sent before stopping the timer
producer.flush()

t1 = time()

# 6. Output the result
print(f"Sent {len(df)} rows.")
print(f'took {(t1 - t0):.2f} seconds')