# Crypto Data Platform Architecture

This project implements a **layered data engineering pipeline** using Airflow, Python, MinIO (S3 compatible storage), and PostgreSQL.

The architecture follows the **Medallion Architecture pattern**:

Bronze → Silver → Gold

This design separates raw ingestion, transformation, and analytics layers.

---

# High-Level Architecture

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

Airflow orchestrates the entire pipeline.

---

# Components

## 1. Airflow DAG (Orchestration Layer)

Location:

```
dags/coin_data_pipeline.py
```

Responsibilities:

- schedule pipeline
- trigger tasks
- manage dependencies
- monitor execution

Pipeline order:

```
create_tables
→ extract_data
→ bronze_layer
→ silver_layer
→ gold_layer
→ warehouse_load
→ validation_checks
```

---

# 2. Extract Layer

Location:

```
pipelines/extract.py
```

Responsibilities:

- call CoinGecko API
- retrieve cryptocurrency market data
- upload raw dataset to object storage

Output:

```
raw/coin_raw.csv
```

---

# 3. Bronze Layer (Raw Storage)

Location:

```
pipelines/bronze_layer.py
```

Responsibilities:

- store raw dataset in S3
- convert CSV to JSON
- create date-partitioned storage

Example path:

```
bronze/coins/dt=2026-03-12/coin_raw.json
```

Purpose:

- preserve raw source data
- allow reprocessing if transformations fail

---

# 4. Silver Layer (Cleaned Data)

Location:

```
pipelines/silver_layer.py
```

Responsibilities:

- transform raw records
- enforce schema using PyArrow
- convert dataset into Parquet format
- apply atomic write pattern

Schema contract:

```
coin_id
symbol
name
price_usd
market_cap
timestamp
```

Output example:

```
silver/coins/dt=2026-03-12/coin_clean.parquet
```

Metadata file:

```
silver/coins/_metadata.json
```

Purpose:

- standardized dataset
- optimized analytics format

---

# 5. Gold Layer (Business Metrics)

Location:

```
pipelines/gold_layer.py
```

Responsibilities:

- compute aggregated metrics
- create analytics-ready datasets

Metrics generated:

```
avg_price_usd
min_price_usd
max_price_usd
avg_market_cap
```

Output example:

```
gold/coins_daily/dt=2026-03-12/coin_daily_metrics.parquet
```

Purpose:

- support dashboards and analytics queries

---

# 6. Data Warehouse (PostgreSQL)

Location:

```
pipelines/warehouse_load.py
```

Responsibilities:

- load gold layer into relational warehouse
- enable analytical queries
- support downstream BI tools

Tables:

```
coin_dimension
coin_prices_fact
gold_coin_daily_metrics
```

---

# 7. Data Quality Validation

Location:

```
utils/validation_utils.py
```

Validation checks include:

- row count verification
- null value checks
- metric sanity checks
- data freshness validation
- SLA validation

Purpose:

- ensure data reliability
- detect pipeline failures early

---

# Utility Modules

Location:

```
utils/
```

Components:

### db_utils.py

Centralized PostgreSQL connection management.

### s3_utils.py

Helper functions for interacting with S3 / MinIO.

### config_loader.py

Loads pipeline configuration from YAML.

### logger.py

Provides structured logging across modules.

---

# Configuration

Location:

```
configs/settings.yaml
```

Contains:

- bucket names
- dataset prefixes
- schema versions
- connection identifiers

Benefits:

- environment flexibility
- no hardcoded values

---

# Technology Stack

| Component | Technology |
|--------|---------|
Orchestration | Apache Airflow |
Storage | MinIO (S3 compatible) |
Processing | Python + Pandas |
Columnar Storage | Parquet |
Schema Validation | PyArrow |
Data Quality | Great Expectations |
Warehouse | PostgreSQL |
Version Control | GitHub |

---

# Repository Structure

```
crypto_data_platform/

configs/
dags/
pipelines/
utils/
docs/
tests/
data/
```

---

# Key Engineering Practices

This project demonstrates several production data engineering practices:

- modular pipeline design
- configuration-driven architecture
- structured logging
- schema enforcement
- atomic data writes
- data validation
- layered data architecture

---

# Future Improvements

Potential upgrades include:

- streaming ingestion using Kafka
- Change Data Capture (CDC)
- Apache Iceberg / Delta Lake table formats
- distributed processing with Spark
- observability dashboards

---

# Conclusion

This project demonstrates how to design a **reliable data platform pipeline** using modern data engineering practices.

It provides a foundation for building scalable analytics systems.
```

---

# What This File Achieves

This file makes your project **much stronger** because it shows:

- system thinking
- architectural clarity
- documentation ability

These are **mid-level engineering traits**.

Most portfolios **do not have architecture docs**.

---

# Next Step

Add the file here:

```text
docs/architecture.md
```

Commit:

```bash
git add .
git commit -m "Added architecture documentation"
git push
```

---

Once that's done, tell me:

```
Architecture doc added
```

Then we will move to **Day 4 — Data Lake Partition Optimization**, which is a **very real production data engineering concept**.