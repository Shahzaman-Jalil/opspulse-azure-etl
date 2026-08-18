import os
import json
import logging
import pandas as pd
from io import BytesIO
from azure.storage.filedatalake import DataLakeServiceClient

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler("etl_log.log"), logging.StreamHandler()]
)
logger = logging.getLogger("opspulse")

# ---------------- CONFIG ----------------
connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
local_file_path = "/data/yellow_tripdata_2024-01.parquet"
watermark_file = "watermark.json"

try:
    # 1. Connect
    logger.info("Connecting to Azure...")
    service_client = DataLakeServiceClient.from_connection_string(connection_string)

    # 2. Read data
    logger.info("Reading source file...")
    df = pd.read_parquet(local_file_path)
    logger.info(f"Total rows: {len(df)}")

    # 3. Read watermark
    if os.path.exists(watermark_file):
        with open(watermark_file, "r") as f:
            last_watermark = json.load(f)["last_watermark"]
    else:
        last_watermark = "1900-01-01 00:00:00"
    logger.info(f"Last watermark: {last_watermark}")

    # 4. Filter new data only (incremental)
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df_new = df[df["tpep_pickup_datetime"] > pd.to_datetime(last_watermark)].copy()
    logger.info(f"New rows to process: {len(df_new)}")

    if len(df_new) == 0:
        logger.info("No new data. Stopping here.")
    else:
        # 5. Upload RAW
        raw_fs = service_client.get_file_system_client(file_system="raw")
        raw_dir = raw_fs.get_directory_client("nyc_taxi/2024/01")
        raw_dir.create_directory()
        raw_file = raw_dir.get_file_client("yellow_tripdata_2024-01.parquet")
        with open(local_file_path, "rb") as data:
            raw_file.upload_data(data, overwrite=True, connection_timeout=600, max_concurrency=1)
        logger.info("Raw file uploaded.")

        # 6. Transform
        df_clean = df_new.copy()
        df_clean["passenger_count"] = df_clean["passenger_count"].fillna(1)
        df_clean["RatecodeID"] = df_clean["RatecodeID"].fillna(99)
        df_clean["store_and_fwd_flag"] = df_clean["store_and_fwd_flag"].fillna("N")
        df_clean["congestion_surcharge"] = df_clean["congestion_surcharge"].fillna(0)
        df_clean["Airport_fee"] = df_clean["Airport_fee"].fillna(0)
        df_clean = df_clean[df_clean["fare_amount"] > 0]
        df_clean = df_clean[df_clean["trip_distance"] > 0]
        logger.info(f"Cleaned data: {len(df_clean)} rows")

        # 7. Upload PROCESSED
        processed_fs = service_client.get_file_system_client(file_system="processed")
        processed_dir = processed_fs.get_directory_client("nyc_taxi/2024/01")
        processed_dir.create_directory()
        processed_file = processed_dir.get_file_client("processed_nyc_taxi_2024_01.csv")
        buffer = BytesIO()
        df_clean.to_csv(buffer, index=False)
        buffer.seek(0)
        processed_file.upload_data(buffer, overwrite=True, connection_timeout=600, max_concurrency=1)
        logger.info("Processed file uploaded.")

        # 8. Update watermark (only if new data existed)
        new_watermark = str(df_new["tpep_pickup_datetime"].max())
        with open(watermark_file, "w") as f:
            json.dump({"last_watermark": new_watermark}, f)
        logger.info(f"Watermark updated: {new_watermark}")

    logger.info("Pipeline completed successfully.")

except Exception as e:
    logger.error(f"Pipeline failed: {e}")
