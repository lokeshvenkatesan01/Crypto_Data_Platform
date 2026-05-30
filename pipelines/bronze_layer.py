import json
import pandas as pd

from datetime import datetime
from io import BytesIO

from utils.logger import get_logger
from utils.s3_utils import get_s3_client

logger = get_logger(__name__)

def upload_raw_to_s3(**context):

    logger.info("Uploading RAW data to Bronze layer")

    execution_date = context["ds"]

    dt = datetime.strptime(execution_date, "%Y-%m-%d")

    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")

    bucket = "crypto-lake"

    s3 = get_s3_client()

    logger.info("Reading raw CSV from MinIO")

    obj = s3.get_object(
        Bucket=bucket,
        Key="raw/coin_raw.csv"
    )

    df = pd.read_csv(
        BytesIO(obj["Body"].read())
    )

    if df.empty:
        raise ValueError("Raw dataset is empty")

    logger.info(f"Loaded {len(df)} records")

    records = df.to_dict(orient="records")

    bronze_key = (
        f"bronze/coins/year={year}/month={month}/day={day}/coin_raw.json"
    )

    s3.put_object(
        Bucket=bucket,
        Key=bronze_key,
        Body=json.dumps(records)
    )

    logger.info(
        f"Bronze dataset written to s3://{bucket}/{bronze_key}"
    )

