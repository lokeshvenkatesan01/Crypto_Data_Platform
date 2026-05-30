from io import BytesIO
from datetime import datetime

import pandas as pd

from utils.logger import get_logger
from utils.s3_utils import get_s3_client


logger = get_logger(__name__)


def build_gold_coin_daily_minio(**context):
    """
    Build Gold layer daily coin metrics from Silver dataset.
    """

    execution_date = context["ds"]

    dt = datetime.strptime(execution_date, "%Y-%m-%d")

    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")

    logger.info("Building GOLD layer (coin daily metrics)")
    logger.info(
        f"Building GOLD layer for execution_date={execution_date}"
    )

    bucket = "crypto-lake"

    # Silver path (must match silver_layer.py)
    silver_key = (
        f"silver/coins/dt={execution_date}/coin_clean.parquet"
    )

    # Gold path
    gold_key = (
        f"gold/coins_daily/year={year}/month={month}/day={day}/coin_daily_metrics.parquet"
    )

    s3 = get_s3_client()

    # --------------------------------------------------
    # Read Silver Dataset
    # --------------------------------------------------

    logger.info(
        f"Reading Silver dataset from {silver_key}"
    )

    obj = s3.get_object(
        Bucket=bucket,
        Key=silver_key
    )

    df = pd.read_parquet(
        BytesIO(
            obj["Body"].read()
        )
    )

    if df.empty:
        raise ValueError(
            "Gold aggregation cannot run on empty dataset"
        )

    logger.info(
        f"Silver dataset row count: {len(df)}"
    )

    # --------------------------------------------------
    # Build Gold Aggregates
    # --------------------------------------------------

    gold_df = (
        df.groupby("coin_id")
        .agg(
            avg_price_usd=("price_usd", "mean"),
            min_price_usd=("price_usd", "min"),
            max_price_usd=("price_usd", "max"),
            avg_market_cap=("market_cap", "mean"),
        )
        .reset_index()
    )

    gold_df["dt"] = execution_date

    logger.info(
        f"Gold dataset row count: {len(gold_df)}"
    )

    # --------------------------------------------------
    # Write Gold Dataset
    # --------------------------------------------------

    buffer = BytesIO()

    gold_df.to_parquet(
        buffer,
        index=False
    )

    buffer.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=gold_key,
        Body=buffer.getvalue()
    )

    logger.info(
        f"GOLD dataset written to s3://{bucket}/{gold_key}"
    )

    logger.info(
        "Gold layer build completed successfully"
    )