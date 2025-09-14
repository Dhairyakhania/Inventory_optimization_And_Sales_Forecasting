# consumer.py
import os
import json
import logging
from kafka import KafkaConsumer
from minio import Minio
from datetime import datetime
import io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SalesConsumer")

# ------------------------------------------------------------------------------
# Environment config
# ------------------------------------------------------------------------------

# Kafka settings
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales-events")

# MinIO settings
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_RAW", "sales-raw")

# Auto-switch if running locally and MINIO_ENDPOINT points to container hostname
if MINIO_ENDPOINT.startswith("minio:"):
    # Default to localhost when running outside Docker
    if os.getenv("RUN_LOCAL", "true").lower() == "true":
        logger.info(f"Switching MinIO endpoint from {MINIO_ENDPOINT} → localhost:9000")
        MINIO_ENDPOINT = "localhost:9000"

# ------------------------------------------------------------------------------
# Initialize MinIO client
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# Kafka consumer setup
# ------------------------------------------------------------------------------
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="sales-consumer-group",
)

logger.info("Listening for sales events...")

# ------------------------------------------------------------------------------
# Consume messages and upload to MinIO
# ------------------------------------------------------------------------------
# for message in consumer:
#     record = message.value
#     file_name = f"sales_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}.json"
#     logger.info(f"Uploading record to MinIO: {file_name}")

#     # Convert record to JSON bytes
#     data_bytes = io.BytesIO(json.dumps(record).encode("utf-8"))

#     # Upload to MinIO
#     minio_client.put_object(
#         bucket_name=MINIO_BUCKET,
#         object_name=file_name,
#         data=data_bytes,
#         length=len(data_bytes.getvalue()),
#         content_type="application/json",
#     )
#     logger.info(f"Successfully uploaded {file_name} to bucket {MINIO_BUCKET}")


for message in consumer:
    record = message.value
    logger.info(f"Consumed: {record}")

    # Get event date (fallback to today if missing)
    event_date = datetime.strptime(record.get("timestamp", datetime.now().strftime("%Y-%m-%d")), "%Y-%m-%d")
    year, month, day = event_date.strftime("%Y"), event_date.strftime("%m"), event_date.strftime("%d")

    # Build partitioned path
    object_name = f"year={year}/month={month}/day={day}/sales_{datetime.now().strftime('%H%M%S%f')}.json"

    # Save record to MinIO
    record_bytes = json.dumps(record).encode("utf-8")
    record_stream = io.BytesIO(record_bytes)
    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=record_stream,
        length=len(record_bytes),
        content_type="application/json"
    )

    logger.info(f"Stored in MinIO: {MINIO_BUCKET}/{object_name}")