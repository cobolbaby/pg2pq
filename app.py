import os
import sys
import time
from datetime import datetime

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

import duckdb

# Load environment variables from .env file if it exists (for local development)
if os.path.exists('.env'):
    load_dotenv()

# Configure environment variables for database connections and GCS
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_INSTANCE = os.getenv("POSTGRES_INSTANCE")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA")
GCS_MOUNT_PATH = os.getenv("GCS_MOUNT_PATH", "/mnt/gcs")
MINIO_ENABLED = os.getenv("MINIO_ENABLED", "false").lower() == "true"  # Set to "true" to enable MinIO uploads
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")  # e.g., 'minio.example.com'
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

try:
    # Create DuckDB connection
    duckdb_conn = duckdb.connect(database=':memory:')

    # Install and load PostgreSQL extension
    print("Installing PostgreSQL extension...", end='', flush=True)
    duckdb_conn.execute("FORCE INSTALL './duckdb/postgres_scanner.duckdb_extension';")
    print(" done.", flush=True)
    
    print("Loading PostgreSQL extension...", end='', flush=True)
    duckdb_conn.execute("LOAD postgres;")
    print(" done.", flush=True)

    # Define PostgreSQL connection string
    postgres_conn_str = f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
    
    # Attach PostgreSQL database
    print(f"Attaching PostgreSQL database {POSTGRES_DB}...", end='', flush=True)
    duckdb_conn.execute(f"ATTACH '{postgres_conn_str}' AS {POSTGRES_DB} (TYPE POSTGRES, READ_ONLY, SCHEMA '{POSTGRES_SCHEMA}');")
    print(" done.", flush=True)

    # Get the list of tables in the PostgreSQL database
    # fix: 通过 duckdb information_schema.tables 获取到的所有 table 类型都是 BASE TABLE
    # tables = duckdb_conn.execute(f"""SELECT table_name FROM information_schema.tables WHERE table_schema='{POSTGRES_SCHEMA}' 
    #                              AND (table_name like 'dim_%' OR table_name like 'fact_%')""").fetchall()

    tables = duckdb_conn.execute(f"""SELECT * FROM postgres_query('{POSTGRES_DB}', 
                                 "SELECT table_name FROM information_schema.tables 
                                 WHERE table_schema = '{POSTGRES_SCHEMA}' AND table_type = 'BASE TABLE';")""").fetchall()                                 

    # Initialize MinIO client if enabled
    if MINIO_ENABLED:
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=True  # Set to True if using HTTPS
        )

        # Ensure bucket exists
        if not minio_client.bucket_exists(MINIO_BUCKET):
            raise Exception(f"Bucket '{MINIO_BUCKET}' does not exist")

    # Export each table to Parquet format
    for table in tables:
        table_name = table[0]

        table_path = os.path.join(GCS_MOUNT_PATH, POSTGRES_INSTANCE, POSTGRES_DB, POSTGRES_SCHEMA, table_name)
        if not os.path.exists(table_path):
            os.makedirs(table_path)

        # 按照快照时间定义文件名称
        snapshot_time = datetime.now().strftime("%Y%m%d%H%M%S")
        table_dump_filename = f"{snapshot_time}-full.parquet"
        table_dump_file = os.path.join(table_path, table_dump_filename)

        try:
            start_time = time.time()
            print(f"Exporting {POSTGRES_DB}.{POSTGRES_SCHEMA}.{table_name} to {table_dump_file} at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}...", end='', flush=True)
            
            duckdb_conn.execute(f"COPY (SELECT * FROM {POSTGRES_DB}.{POSTGRES_SCHEMA}.{table_name}) TO '{table_dump_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);")

            # Conditionally upload to MinIO if enabled
            if MINIO_ENABLED:
                minio_client.fput_object(MINIO_BUCKET, f"{POSTGRES_INSTANCE}/{POSTGRES_SCHEMA}/{table_name}/{table_dump_filename}", table_dump_file)
                print(f" Uploaded to MinIO: {MINIO_BUCKET}/{POSTGRES_INSTANCE}/{POSTGRES_SCHEMA}/{table_name}/{table_dump_filename}", end='', flush=True)

            duration = time.time() - start_time
            print(f" done. (Duration: {duration:.2f} seconds)", flush=True)

        except S3Error as s3_error:
            print(f" MinIO Error: {s3_error}")
        except Exception as e:
            print(f" Error: {e}")

    print("Database exported successfully", end='')
    if MINIO_ENABLED:
        print(" and uploaded to MinIO.")
    else:
        print(".")
except Exception as e:
    print(f"Error: {e}")
