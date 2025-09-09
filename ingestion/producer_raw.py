# producer_sales.py
import os
import json
import logging
import pandas as pd
from kafka import KafkaProducer
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from synthetic_data.data_generator import RealisticSalesDataGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SalesProducer")

# Kafka producer config
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
kafka_topic = "sales-events"

# Generate synthetic data
data_gen = RealisticSalesDataGenerator(start_date="2023-01-01", end_date="2023-01-07")
file_paths = data_gen.generate_sales_data(output_dir="./synthetic_output")

logger.info("Streaming sales data to Kafka...")

# Stream sales records row by row
for sales_file in file_paths['sales']:
    df = pd.read_parquet(sales_file)
    for _, row in df.iterrows():
        record = row.to_dict()
        try:
            producer.send(kafka_topic, value=record)
        except Exception as e:
            logger.error(f"Failed to send record to Kafka: {e}")

producer.flush()
logger.info("All records sent to Kafka successfully.")
