def load_gold_to_postgres(**context):
    import logging
    from io import BytesIO
    import pandas as pd
    from psycopg2.extras import execute_values
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    logging.info("Loading GOLD dataset into Postgres")

    execution_date = context["ds"]
    bucket = "crypto-lake"

    gold_key = f"gold/coins_daily/dt={execution_date}/coin_daily_metrics.parquet"

    # 1️⃣ Read Gold Parquet from MinIO
    s3 = S3Hook(aws_conn_id="minio_s3")
    obj = s3.get_key(gold_key, bucket_name=bucket)
    df = pd.read_parquet(BytesIO(obj.get()["Body"].read()))

    if df.empty:
        raise ValueError("❌ Gold dataset is empty — aborting Postgres load")

    # 2️⃣ Prepare records
    records = [
        (
            execution_date,
            r.coin_id,
            r.avg_price_usd,
            r.min_price_usd,
            r.max_price_usd,
            r.avg_market_cap,
        )
        for r in df.itertuples(index=False)
    ]

    # 3️⃣ Insert / upsert into Postgres
    conn = get_pg_conn()
    cur = conn.cursor()

    sql = """
        INSERT INTO gold_coin_daily_metrics
        (dt, coin_id, avg_price_usd, min_price_usd, max_price_usd, avg_market_cap)
        VALUES %s
        ON CONFLICT (dt, coin_id)
        DO UPDATE SET
            avg_price_usd   = EXCLUDED.avg_price_usd,
            min_price_usd   = EXCLUDED.min_price_usd,
            max_price_usd   = EXCLUDED.max_price_usd,
            avg_market_cap  = EXCLUDED.avg_market_cap;
    """

    execute_values(cur, sql, records, page_size=1000)

    conn.commit()
    cur.close()
    conn.close()

    logging.info("GOLD dataset successfully loaded into Postgres")
  
