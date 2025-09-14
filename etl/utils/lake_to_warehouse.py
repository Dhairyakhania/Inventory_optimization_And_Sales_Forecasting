import logging
import pandas as pd
from io import BytesIO
from minio import Minio
from sqlalchemy import create_engine
import os
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
# MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
# MINIO_BUCKET = os.getenv("MINIO_BUCKET_RAW", "sales-raw")

PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "warehouse")
# TARGET_TABLE = "fact_sales"

TABLE_NAME = "warehouse.sales"   # <-- use schema.table

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

def get_minio_client():
    # Detect whether running inside Docker
    running_in_docker = os.path.exists("/.env")

    if running_in_docker:
        minio_host = os.getenv("MINIO_ENDPOINT", "minio:9000")
    else:
        minio_host = os.getenv("MINIO_HOST", "localhost:9000")

    print(f"[INFO] Connecting to MinIO at {minio_host}")

    client = Minio(
        minio_host,
        access_key=os.getenv("MINIO_ROOT_USER", "minio"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
        secure=False
    )
    return client

def get_postgres_connection():
    running_in_docker = os.path.exists("/.env")

    if running_in_docker:
        host = os.getenv("POSTGRES_HOST", "postgres")   # service name in docker
    else:
        host = "localhost"  # when running locally

    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "warehouse"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )
    return conn

# client = Minio(
#     MINIO_ENDPOINT,
#     access_key=MINIO_ACCESS_KEY,
#     secret_key=MINIO_SECRET_KEY,
#     secure=False
# )
# ---------------------------
# STEP 1: Fetch latest file from MinIO
# ---------------------------
import json

def fetch_latest_from_minio(bucket: str) -> pd.DataFrame:
    
    client = get_minio_client()

    objects = list(client.list_objects(bucket, recursive=True))
    if not objects:
        raise Exception(f"No objects found in bucket {bucket}")

    # pick latest object by last_modified
    latest_obj = max(objects, key=lambda x: x.last_modified)
    print(f"[INFO] Fetching latest file: {latest_obj.object_name}")

    response = client.get_object(bucket, latest_obj.object_name)
    raw_data = response.read().decode("utf-8")

    # Handle single-line JSON objects
    try:
        data = json.loads(raw_data)
        if isinstance(data, dict):
            df = pd.DataFrame([data])
        else:
            df = pd.DataFrame(data)
    except json.JSONDecodeError:
        # Handle newline-delimited JSON
        df = pd.read_json(BytesIO(raw_data.encode("utf-8")), lines=True)

    return df

# ---------------------------
# STEP 2: Transformations
# ---------------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    print("[INFO] Transforming data...")
    # Example transformations
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.month
    df["year"] = df["date"].dt.year
    df["profit_margin"] = df["profit"] / df["revenue"]
    return df

# ---------------------------
# STEP 3: Load into PostgreSQL
# ---------------------------
def create_schema_if_not_exists(conn, schema_name="warehouse"):
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    conn.commit()



def load_to_postgres(df: pd.DataFrame, table_name: str, schema_name: str = "warehouse"):
    # conn = get_postgres_connection()
    # cur = conn.cursor()
    engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")
    conn = engine.raw_connection()
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    full_table_name = f"{schema_name}.{table_name}"

    # create_table_query = f"""
    # CREATE TABLE IF NOT EXISTS {full_table_name} (
    #     id SERIAL PRIMARY KEY,
    #     date TIMESTAMP NOT NULL,
    #     store_id VARCHAR(50),
    #     product_id VARCHAR(50),
    #     category VARCHAR(100),
    #     quantity_sold INT,
    #     unit_price NUMERIC(10,2),
    #     discount_percent NUMERIC(5,2),
    #     revenue NUMERIC(12,2),
    #     cost NUMERIC(12,2),
    #     profit NUMERIC(12,2)
    # );
    # """
    # cur.execute(create_table_query)

    # with engine.begin() as conn:
    #     conn.execute(create_table_query)
    for _, row in df.iterrows():
        cur.execute(
            f"""
            INSERT INTO {full_table_name} 
            (date, store_id, product_id, category, quantity_sold, unit_price, discount_percent, revenue, cost, profit)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                row["date"],
                row["store_id"],
                row["product_id"],
                row["category"],
                row["quantity_sold"],
                row["unit_price"],
                row["discount_percent"],
                row["revenue"],
                row["cost"],
                row["profit"],
            )
        )
    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"Loaded {len(df)} records into {table_name}")

# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":
    MINIO_BUCKET = os.getenv("MINIO_BUCKET_RAW", "sales-raw")
    TABLE_NAME = "sales"

    df = fetch_latest_from_minio(MINIO_BUCKET)
    df = transform(df)
    load_to_postgres(df, TABLE_NAME)
