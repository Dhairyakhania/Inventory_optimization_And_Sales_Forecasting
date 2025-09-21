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
logger = logging.getLogger("SalesProducerKafka")

OUTPUT_DIR = "./synthetic_output"
KAFKA_TOPIC = "sales-events"
KAFKA_BOOTSTRAP = "localhost:9092"


def json_serializer(obj):
    """Custom serializer for Pandas Timestamps etc."""
    if isinstance(obj, (pd.Timestamp, )):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def main():
    # Step 1 — Generate synthetic Parquet files
    data_gen = RealisticSalesDataGenerator(
        start_date="2023-01-01",
        end_date="2025-01-07"
    )
    file_paths = data_gen.generate_sales_data(output_dir=OUTPUT_DIR)
    logger.info(f"✅ Sales data written to {OUTPUT_DIR}")

    # Step 2 — Connect Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=json_serializer).encode("utf-8")
    )

    # Step 3 — Read each Parquet and stream rows
    for parquet_file in file_paths['sales']:
        logger.info(f"📂 Reading {parquet_file}")
        df = pd.read_parquet(parquet_file)

        for _, row in df.iterrows():
            record = row.to_dict()
            try:
                producer.send(KAFKA_TOPIC, value=record)
            except Exception as e:
                logger.error(f"❌ Failed to send record: {e}")

    producer.flush()
    logger.info("🚀 All records successfully streamed to Kafka.")


if __name__ == "__main__":
    main()