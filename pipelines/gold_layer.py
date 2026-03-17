def build_gold_coin_daily_minio(**context):
    import logging
    from io import BytesIO
    from utils.logger import get_logger
    import pandas as pd
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from datetime import datetime
    logger = get_logger(__name__)
    execution_date = context["ds"]
    dt = datetime.strptime(execution_date, "%Y-%m-%d")
    
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    
    logging.info("Building GOLD layer (coin daily metrics)")
    logger.info(f"Building GOLD layer for execution_date={execution_date}")
   
    bucket = "crypto-lake"

    silver_key = (
        f"silver/coins/year={year}/month={month}/day={day}/coin_clean.parquet"
    )
    gold_key = f"gold/coins_daily/year={year}/month={month}/day={day}/coin_daily_metrics.parquet"

    s3 = S3Hook(aws_conn_id="minio_s3")
    
    

    # 1️⃣ Read Silver Parquet
    silver_obj = s3.get_key(silver_key, bucket_name=bucket)
    df = pd.read_parquet(BytesIO(silver_obj.get()["Body"].read()))

    if df.empty:
        raise ValueError("Gold aggregation cannot run on empty dataset")

    # 2️⃣ Aggregate (Gold logic)
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

    # 3️⃣ Add business date
    gold_df["dt"] = execution_date

    # 4️⃣ Write Gold Parquet (idempotent overwrite)
    buffer = BytesIO()
    gold_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3.load_file_obj(
        buffer,
        key=gold_key,
        bucket_name=bucket,
        replace=True,
    )

    logging.info(f"GOLD dataset written to s3://{bucket}/{gold_key}")

