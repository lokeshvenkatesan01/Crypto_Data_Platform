from io import BytesIO
from datetime import datetime

import pandas as pd

from utils.db_utils import bulk_insert
from utils.config_loader import load_config
from utils.logger import get_logger
from utils.s3_utils import get_s3_client
from utils.retry_utils import retry


config = load_config()
logger = get_logger(__name__)


@retry(max_attempts=3, delay_seconds=3)
def load_gold_to_postgres(**context):
    """
    Load GOLD dataset from MinIO into PostgreSQL warehouse.
    """

    execution_date = context["ds"]

    dt = datetime.strptime(
        execution_date,
        "%Y-%m-%d"
    )

    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")

    logger.info("Starting warehouse load process")
    logger.info(
        f"Loading GOLD data into warehouse for execution_date={execution_date}"
    )

    bucket = config["data_lake_bucket"]

    gold_key = (
        f"gold/coins_daily/year={year}/month={month}/day={day}/coin_daily_metrics.parquet"
    )

    logger.info(
        f"Reading GOLD dataset from: {gold_key}"
    )

    s3 = get_s3_client()

    obj = s3.get_object(
        Bucket=bucket,
        Key=gold_key
    )

    df = pd.read_parquet(
        BytesIO(
            obj["Body"].read()
        )
    )

    if df.empty:
        raise ValueError(
            "Gold dataset is empty - No data available for warehouse load"
        )

    logger.info(
        f"Loaded dataframe with {len(df)} rows"
    )

    records = [
        (
            execution_date,
            row.coin_id,
            row.avg_price_usd,
            row.min_price_usd,
            row.max_price_usd,
            row.avg_market_cap,
        )
        for row in df.itertuples(index=False)
    ]

    if len(records) == 0:
        raise ValueError(
            "No records generated for insertion"
        )

    logger.info(
        f"Preparing to insert {len(records)} records"
    )

    bulk_insert(
        table="gold_coin_daily_metrics",
        columns=[
            "dt",
            "coin_id",
            "avg_price_usd",
            "min_price_usd",
            "max_price_usd",
            "avg_market_cap",
        ],
        records=records,
    )

    logger.info(
        "Warehouse load completed successfully"
    )