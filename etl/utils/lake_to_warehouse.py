import os
import json
import pandas as pd
from io import BytesIO
from minio import Minio
import psycopg2
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import time
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
import psycopg2.extras as extras

load_dotenv()

# -------------------------------
# Config
# -------------------------------
MINIO_BUCKET = os.getenv("MINIO_BUCKET_RAW", "sales-raw")

PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "warehouse")

SCHEMA_NAME = "warehouse"
TABLE_NAME = "sales"

# -------------------------------
# Detect environment
# -------------------------------
RUNNING_IN_DOCKER = os.path.exists("/.dockerenv") or os.path.exists("/.env")
PG_HOST = os.getenv("POSTGRES_HOST") or ("postgres" if RUNNING_IN_DOCKER else "localhost")

print(f"[INFO] Running in Docker: {RUNNING_IN_DOCKER}, Postgres host: {PG_HOST}")

# -------------------------------
# Check Postgres connectivity
# -------------------------------
def wait_for_postgres(retries=5, delay=5):
    for i in range(retries):
        try:
            conn = psycopg2.connect(
                dbname=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD,
                host=PG_HOST,
                port=PG_PORT,
            )
            conn.close()
            print("[INFO] Postgres is reachable!")
            return
        except psycopg2.OperationalError:
            print(f"[WARN] Postgres not ready, retrying in {delay}s... ({i+1}/{retries})")
            time.sleep(delay)
    raise Exception("Postgres is not reachable after retries!")

# -------------------------------
# MinIO Client
# -------------------------------
def get_minio_client():
    minio_host = os.getenv("MINIO_ENDPOINT") if RUNNING_IN_DOCKER else os.getenv("MINIO_HOST")
    minio_host = minio_host or ("minio:9000" if RUNNING_IN_DOCKER else "localhost:9000")
    print(f"[INFO] Connecting to MinIO at {minio_host}")
    return Minio(
        minio_host,
        access_key=os.getenv("MINIO_ROOT_USER", "minio"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
        secure=False
    )

# -------------------------------
# PostgreSQL Engine
# -------------------------------
def get_postgres_engine():
    conn_str = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(conn_str)

# -------------------------------
# Read from MinIO
# -------------------------------
def read_from_minio(bucket_name):
    client = get_minio_client()
    objects = list(client.list_objects(bucket_name, recursive=True))

    dfs = []
    for obj in objects:
        if obj.object_name.endswith(".json"):
            response = client.get_object(bucket_name, obj.object_name)
            raw_data = response.read().decode("utf-8")
            try:
                data = json.loads(raw_data)
                df = pd.DataFrame([data]) if isinstance(data, dict) else pd.DataFrame(data)
            except json.JSONDecodeError:
                df = pd.read_json(BytesIO(raw_data.encode("utf-8")), lines=True)
            dfs.append(df)

    if dfs:
        full_df = pd.concat(dfs, ignore_index=True)
        print(f"[INFO] Read {len(full_df)} rows from MinIO bucket '{bucket_name}'")
        return full_df
    else:
        print(f"[WARN] No JSON files found in bucket '{bucket_name}'")
        return pd.DataFrame()

# -------------------------------
# Transform DataFrame
# -------------------------------
def transform(df):
    if "date" not in df.columns:
        raise ValueError("DataFrame must contain 'date' column")
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.month
    df["year"] = df["date"].dt.year
    if "profit" in df.columns and "revenue" in df.columns:
        df["profit_margin"] = df["profit"] / df["revenue"]
    else:
        df["profit_margin"] = None
    return df

# -------------------------------
# Load to Postgres
# -------------------------------
def load_to_postgres(df, table_name, schema_name):
    if df.empty:
        print("[WARN] No data to load")
        return

    engine = get_postgres_engine()
    upserted = 0

    with engine.begin() as conn:
        # Ensure required columns exist
        conn.execute(text(f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN IF NOT EXISTS month INT;"))
        conn.execute(text(f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN IF NOT EXISTS year INT;"))
        conn.execute(text(f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN IF NOT EXISTS profit_margin NUMERIC(12,4);"))

        df = df.drop_duplicates(subset=["date", "store_id", "product_id"], keep="last")

        # Build insert query with ON CONFLICT
        cols = df.columns.tolist()
        columns = ",".join(cols)
        placeholders = ",".join([f"%({c})s" for c in cols])

        conflict_cols = ["date", "store_id", "product_id"]
        updates = ",".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in conflict_cols])

        insert_query = f"""
        INSERT INTO {schema_name}.{table_name} ({columns})
        VALUES %s
        ON CONFLICT ({",".join(conflict_cols)})
        DO UPDATE SET {updates};
        """

        # Convert dataframe to list of tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]

        with conn.connection.cursor() as cur:
            extras.execute_values(
                cur, insert_query, data_tuples, template=None, page_size=1000
            )
            upserted = len(data_tuples)

        print(f"[INFO] Upserted {upserted} rows into {schema_name}.{table_name}")
# -------------------------------
# MAIN
# -------------------------------
if __name__ == "__main__":
    wait_for_postgres()  # Ensure DB is up before loading
    df = read_from_minio(MINIO_BUCKET)
    if not df.empty:
        df = transform(df)
        load_to_postgres(df, TABLE_NAME, SCHEMA_NAME)
