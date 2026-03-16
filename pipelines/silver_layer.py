from utils.config_loader import load_config
from utils.logger import get_logger
from utils.validation_utils import (
    validate_not_empty,
    validate_no_nulls,
    validate_positive_values,
)

config = load_config()
logger = get_logger(__name__)

def transform_bronze_to_silver(**context):
    import logging
    import io
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    logging.info("Starting Bronze ➜ Silver transformation (atomic)")

    s3 = S3Hook(aws_conn_id="minio_s3")

    execution_date = context["ds"]
    bucket = "crypto-lake"

    bronze_key = f"bronze/coins/dt={execution_date}/coin_raw.json"
    tmp_silver_key = f"silver/coins/_tmp_dt={execution_date}/coin_clean.parquet"
    final_silver_key = f"silver/coins/dt={execution_date}/coin_clean.parquet"

    # 1️⃣ Read Bronze
    raw_json = s3.read_key(key=bronze_key, bucket_name=bucket)
    df = pd.read_json(io.StringIO(raw_json))
    
    validate_not_empty(df, "silver_coins")

    validate_no_nulls(df, ["coin_id", "price_usd", "market_cap"])

    validate_positive_values(df, ["price_usd", "market_cap"])

    # 2️⃣ Transform
    df = df[
        ["id", "symbol", "name", "current_price", "market_cap", "last_updated"]
    ].rename(
        columns={
            "id": "coin_id",
            "current_price": "price_usd",
            "last_updated": "timestamp",
        }
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["price_usd"] = df["price_usd"].astype(float)
    df["market_cap"] = df["market_cap"].astype(float)

    # # 3️⃣ Write Parquet to TEMP location
    # table = pa.Table.from_pandas(df)
    
    # 3️⃣ Enforce Silver schema (HARD CONTRACT)
    try:
        table = pa.Table.from_pandas(
            df,
            schema=SILVER_SCHEMA_V1,
            preserve_index=False,
            safe=True
        )
    except Exception as e:
        logging.error("❌ Silver schema validation failed")
        logging.error(str(e))
        raise
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    s3.load_bytes(
        bytes_data=buffer.getvalue(),
        key=tmp_silver_key,
        bucket_name=bucket,
        replace=True,
    )

    logging.info(f"Temporary Silver written: {tmp_silver_key}")

    # 4️⃣ Promote TEMP ➜ FINAL (atomic visibility)
    if s3.check_for_key(final_silver_key, bucket):
        s3.delete_objects(bucket=bucket, keys=[final_silver_key])

    s3.copy_object(
        source_bucket_key=tmp_silver_key,
        dest_bucket_key=final_silver_key,
        source_bucket_name=bucket,
        dest_bucket_name=bucket,
    )

    s3.delete_objects(bucket=bucket, keys=[tmp_silver_key])

    logging.info(f"Silver partition committed: {final_silver_key}")
    
    # 5️⃣ Write Silver metadata (schema versioning)
    SILVER_SCHEMA_VERSION = "v1"

    metadata = {
        "dataset": "coins",
        "schema_version": SILVER_SCHEMA_VERSION,
        "execution_date": execution_date,
        "row_count": len(df),
        "source_bronze_path": bronze_key,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    metadata_key = f"silver/coins/dt={execution_date}/_metadata.json"

    s3.load_string(
        string_data=json.dumps(metadata, indent=2),
        key=metadata_key,
        bucket_name=bucket,
        replace=True,
    )

    logging.info(f"Silver metadata written: {metadata_key}")



