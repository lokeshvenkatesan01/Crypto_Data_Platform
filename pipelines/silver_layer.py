from utils.config_loader import load_config
from utils.logger import get_logger
from datetime import datetime, timezone
from utils.retry_utils import retry
import json

from utils.validation_utils import (
    validate_not_empty,
    validate_no_nulls,
    validate_positive_values,
)

config = load_config()
logger = get_logger(__name__)

SILVER_SCHEMA_V1 = "1.0"


@retry(max_attempts=3, delay_seconds=3)
def transform_bronze_to_silver(**context):

    import logging
    import io
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    from utils.s3_utils import get_s3_client
    from datetime import datetime

    execution_date = context["ds"]

    dt = datetime.strptime(execution_date, "%Y-%m-%d")

    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")

    logging.info("Starting Bronze ➜ Silver transformation")

    bucket = "crypto-lake"

    bronze_key = (
        f"bronze/coins/year={year}/month={month}/day={day}/coin_raw.json"
    )

    final_silver_key = (
        f"silver/coins/dt={execution_date}/coin_clean.parquet"
    )

    s3 = get_s3_client()

    # --------------------------------------------------
    # Read Bronze JSON
    # --------------------------------------------------

    obj = s3.get_object(
        Bucket=bucket,
        Key=bronze_key
    )

    raw_json = obj["Body"].read().decode("utf-8")

    df = pd.read_json(io.StringIO(raw_json))

    # --------------------------------------------------
    # Rename columns
    # --------------------------------------------------

    df = df[
        [
            "id",
            "symbol",
            "name",
            "current_price",
            "market_cap",
            "last_updated",
        ]
    ].rename(
        columns={
            "id": "coin_id",
            "current_price": "price_usd",
            "last_updated": "timestamp",
        }
    )

    # --------------------------------------------------
    # Validations
    # --------------------------------------------------

    validate_not_empty(df, "silver_coins")

    validate_no_nulls(
        df,
        [
            "coin_id",
            "price_usd",
            "market_cap",
        ],
    )

    validate_positive_values(
        df,
        [
            "price_usd",
            "market_cap",
        ],
    )

    # --------------------------------------------------
    # Transform
    # --------------------------------------------------

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df["price_usd"] = df["price_usd"].astype(float)

    df["market_cap"] = df["market_cap"].astype(float)

    logger.info(f"Silver dataframe rows: {len(df)}")

    # --------------------------------------------------
    # Convert to Parquet
    # --------------------------------------------------

    table = pa.Table.from_pandas(
        df,
        preserve_index=False,
    )

    buffer = io.BytesIO()

    pq.write_table(table, buffer)

    buffer.seek(0)

    # --------------------------------------------------
    # Write Silver Dataset
    # --------------------------------------------------

    s3.put_object(
        Bucket=bucket,
        Key=final_silver_key,
        Body=buffer.getvalue(),
    )

    logging.info(
        f"Silver dataset written: s3://{bucket}/{final_silver_key}"
    )

    # --------------------------------------------------
    # Metadata
    # --------------------------------------------------

    metadata = {
        "dataset": "coins",
        "schema_version": SILVER_SCHEMA_V1,
        "execution_date": execution_date,
        "row_count": len(df),
        "source_bronze_path": bronze_key,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    metadata_key = (
        f"silver/coins/year={year}/month={month}/day={day}/_metadata.json"
    )

    s3.put_object(
        Bucket=bucket,
        Key=metadata_key,
        Body=json.dumps(metadata, indent=2),
    )

    logging.info(
        f"Silver metadata written: s3://{bucket}/{metadata_key}"
    )

    logger.info("Silver transformation completed successfully")