# Crypto Data Platform

End-to-end data engineering pipeline demonstrating
Bronze → Silver → Gold architecture using Airflow,
Python, MinIO (S3), and PostgreSQL.

## Data Lake Partition Strategy

The data lake follows hierarchical partitioning:

silver/coins/year=YYYY/month=MM/day=DD/

This improves query performance for engines such as:
Athena, Spark, and Trino.