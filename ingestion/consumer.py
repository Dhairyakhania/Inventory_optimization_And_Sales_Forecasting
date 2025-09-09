# consumer_to_minio.py
import os
import json
import logging
from kafka import KafkaConsumer
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MinIOConsumer")

# Kafka consumer config
consumer = KafkaConsumer(
    'sales-events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# MinIO config
s3 = boto3.client(
    's3',
    endpoint_url=os.environ.get('MINIO_ENDPOINT'),
    aws_access_key_id=os.environ.get('MINIO_ROOT_USER'),
    aws_secret_access_key=os.environ.get('MINIO_ROOT_PASSWORD')
)
bucket_name = 'raw-sales-data'

# Create bucket if it doesn't exist
try:
    s3.create_bucket(Bucket=bucket_name)
except s3.exceptions.BucketAlreadyOwnedByYou:
    pass

logger.info("Consuming from Kafka and storing into MinIO...")

for message in consumer:
    record = message.value
    date_str = str(record['date']).split(" ")[0]
    key = f"{date_str}/{record['store_id']}/{record['product_id']}.json"

    try:
        s3.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(record))
        logger.info(f"Saved {key} to MinIO")
    except ClientError as e:
        logger.error(f"Failed to upload {key} to MinIO: {e}")
