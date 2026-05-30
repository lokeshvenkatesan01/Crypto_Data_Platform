FROM apache/airflow:2.8.1

USER airflow

RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    psycopg2-binary \
    boto3 \
    requests \
    PyYAML