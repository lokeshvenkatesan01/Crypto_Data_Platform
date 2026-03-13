def upload_raw_to_s3(**context):
    logging.info("Uploading RAW data to MinIO (Bronze layer)")

    ds = context["ds"]  # execution date
    local_path = f"{DATA_DIR}/coin_raw.csv"

    df = pd.read_csv(local_path)
    records = df.to_dict(orient="records")

    s3_key = f"bronze/coins/dt={ds}/coin_raw.json"

    s3 = S3Hook(aws_conn_id="minio_s3")

    s3.load_string(
        string_data=json.dumps(records),
        key=s3_key,
        bucket_name="crypto-lake",
        replace=True,
    )

    logging.info(f"Uploaded RAW data to s3://crypto-lake/{s3_key}")

