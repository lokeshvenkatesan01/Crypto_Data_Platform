CoinGecko API
      ↓
Airflow DAG
      ↓
Bronze Layer (JSON)
      ↓
Silver Layer (Parquet + schema)
      ↓
Gold Layer (Aggregations)
      ↓
PostgreSQL Warehouse

===========================

API (CoinGecko)
      ↓
Extract Layer
      ↓
Bronze Layer (JSON)
      ↓
Silver Layer (Parquet + Schema)
      ↓
Gold Layer (Aggregations)
      ↓
PostgreSQL Warehouse
      ↓
Validation Checks