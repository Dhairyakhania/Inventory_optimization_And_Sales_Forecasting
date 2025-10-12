# consumer.py
import os
import json
import logging
from kafka import KafkaConsumer
from minio import Minio
from datetime import datetime
import io

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SalesConsumer")

# -------------------------
# Environment config
# -------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales-events")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_RAW", "sales-raw")

# Auto-switch if running locally
if MINIO_ENDPOINT.startswith("minio:") and os.getenv("RUN_LOCAL", "true").lower() == "true":
    logger.info(f"Switching MinIO endpoint from {MINIO_ENDPOINT} → localhost:9000")
    MINIO_ENDPOINT = "localhost:9000"

# -------------------------
# Initialize MinIO client
# -------------------------
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)

# Ensure bucket exists
if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)
    logger.info(f"Created bucket: {MINIO_BUCKET}")
else:
    logger.info(f"Bucket {MINIO_BUCKET} already exists")

# -------------------------
# Kafka consumer setup
# -------------------------
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="sales-consumer-group",
)

logger.info("Listening for sales events...")

# -------------------------
# Consume messages and upload to MinIO
# -------------------------
for message in consumer:
    try:
        record = message.value
        logger.info(f"Consumed record: {record}")

        # Get event timestamp or fallback to now
        timestamp_str = record.get("timestamp") or record.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
        try:
            event_date = datetime.strptime(timestamp_str, "%Y-%m-%d")
        except ValueError:
            # Handle full datetime string if provided
            event_date = datetime.strptime(timestamp_str[:10], "%Y-%m-%d")

        year, month, day = event_date.strftime("%Y"), event_date.strftime("%m"), event_date.strftime("%d")

        # Build partitioned object name
        object_name = f"year={year}/month={month}/day={day}/sales_{datetime.utcnow().strftime('%H%M%S%f')}.json"

        # Convert record to bytes
        record_bytes = json.dumps(record).encode("utf-8")
        record_stream = io.BytesIO(record_bytes)

        # Upload to MinIO
        minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
            data=record_stream,
            length=len(record_bytes),
            content_type="application/json"
        )

        logger.info(f"Stored in MinIO: {MINIO_BUCKET}/{object_name}")

    except Exception as e:
        logger.error(f"Failed to process record: {e}")
