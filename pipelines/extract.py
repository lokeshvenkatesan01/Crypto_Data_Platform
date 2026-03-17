from utils.logger import get_logger
from utils.config_loader import load_config
import requests
import pandas as pd
import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from utils.retry_utils import retry

config = load_config()
logger = get_logger(__name__)


@retry(max_attempts=3, delay_seconds=5)
def extract_data():

    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc"}

    logger.info("Fetching crypto data from CoinGecko")

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    df = pd.DataFrame(response.json())
    
    if df.empty:
        raise ValueError("Extracted dataset is empty")

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_hook = S3Hook(aws_conn_id="minio_s3")

    bucket = config["data_lake_bucket"]

    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key="raw/coin_raw.csv",
        bucket_name=bucket,
        replace=True,
    )

    logger.info("Raw data uploaded to S3")