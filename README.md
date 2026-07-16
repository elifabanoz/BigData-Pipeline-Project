# Big Data Analytics Pipeline — Olist E-Commerce

A hands-on big data project built around the
[Olist Brazilian E-Commerce public dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) —
~100,000 real orders from Brazil's largest online marketplace (2016–2018).

The project is developed in stages. Each stage adds a new layer to the pipeline. More stages will be added over time.

---

## 📌 Important Notes

### Submission
Each student must **fork or clone this repository**, implement their solution, and submit by **opening a Pull Request (PR) back to this repository** with their completed work. PRs are the only accepted submission method.

### Docker is Optional
The Docker Compose files and scripts provided in this repo are **starter code only** — a reference setup to help you get up and running quickly. You are **not required** to use Docker. Feel free to run HDFS, Spark, and Superset however you prefer (local install, cloud, a different container setup, etc.), as long as the pipeline works end-to-end.

---

## Architecture (Phase 1)

```
[Olist Dataset — 9 CSV Tables]
        |
        v
[Apache Spark]
  · Reads CSVs
  · Writes Parquet
        |
        v
[HDFS or MinIO]
        |
        v
[Apache Superset — Simple Charts]
```

---


## Phases

### ✅ Phase 1 — Ingest & Visualize

> **Current task**

- Download the Olist dataset (9 CSV tables).
- Import all CSVs into **HDFS or MinIO** in **Parquet format** using Apache Spark.
- Connect Apache Superset to the stored data and create a few simple charts/diagrams.

No advanced transformations are required for this phase.

---

### ✅ Phase 2 — Building Data Pipeline
From a business perspective, the driving incentive for creating data pipelines is to design a system to transform their most valuable asset –raw data –into actionable information

<img width="737" height="883" alt="image" src="https://github.com/user-attachments/assets/6b6e5f56-0825-49bf-b53f-2007ad6d423b" />


| File | Rows | Description |
|---|---|---|
| olist_orders_dataset.csv | ~100k | Order lifecycle: timestamps, status |
| olist_order_items_dataset.csv | ~112k | Items: product, seller, price, freight |
| olist_order_payments_dataset.csv | ~104k | Payment type, installments, value |
| olist_order_reviews_dataset.csv | ~100k | Review score (1–5) and comment |
| olist_customers_dataset.csv | ~100k | Customer city, state, zip code |
| olist_sellers_dataset.csv | ~3k | Seller city, state, zip code |
| olist_products_dataset.csv | ~33k | Category, dimensions, weight |
| olist_geolocation_dataset.csv | ~1M | ZIP code → lat/lng |
| product_category_name_translation.csv | ~71 | Portuguese → English category names |


STUDY:
- Is data clean? Explain what clean data means? any duplicate entities in it? if so de-duplicate.
- Based on given input data above, what should our output look like to reply the business questions below? Reason about it.
- ETL or ELT approach you adopted to transform the data, why? Learn more about dbt and explain.
- Explain the layers and / or the logic in your architecture.
- Create star schema(s), explain fact & dimension tables and how they can respond the questions
- Python / sql or both you can use, up to you:



| Business Question | Fact Table | Dimensions |
|---|---|---|
| Monthly revenue | | |
| Revenue by product category | | |
| Top-performing sellers | | |
| Sales by customer state | | |
| Average delivery time by state | | |
| Payment method trends | | |
| Average review score by category | | |

---

### ✅ Phase 3 — Re-Construct The Data Pipeline
**Phase 3 will focus on two main objectives:**
1. Integrate Apache Airflow as an orchestrator to automate, schedule, and monitor data pipeline jobs:
2. Implement dbt (Data Build Tool) to handle data transformations and establish a Medallion Architecture.

<img width="1060" height="418" alt="image" src="https://github.com/user-attachments/assets/ad57ffb8-bd0e-42b9-93e1-32e949547abd" />

**STUDY:**
1. Explain Apache Airflow core components with 1 sentence
2. Automate data ingestion and dbt (star schema) transformations with Airflow:
- Build your DAG,
- Provide a walk-through explaining your architectural decisions.
- Define explicit boundaries, operator choices, and resource configurations for each task within the workflow.
4. Migrate and re-build your existing star schema into a dbt (Data Build Tool) project.
- You will restructure the data pipeline into a multi-layered Medallion Architecture (Bronze, Silver, Gold) to enforce clean data boundaries, improve data quality, and modularize your transformations.
- Depict it in a diagram or visualize it as you wish. Explore the benefits of the architecture.
5. Explain the challenging parts of this reconstruction.







---

## Docker Quick Start (Optional)

The following commands use the provided Docker Compose files as a starting point.

**1. Create the shared network**

```bash
# Linux / macOS
bash scripts/setup_network.sh

# Windows (PowerShell)
.\scripts\setup_network.ps1
```

**2. Start the services**

```bash
docker compose -f docker/docker-compose-hdfs.yml up -d
docker compose -f docker/docker-compose-spark.yml up -d
docker compose -f docker/docker-compose-superset.yml up -d
```

| Service         | URL                       | Credentials   |
|-----------------|---------------------------|---------------|
| HDFS NameNode   | http://localhost:9870     |               |
| Spark Master    | http://localhost:8082     |               |
| Superset        | http://localhost:8088     | admin / admin |

**3. Stop everything**

```bash
docker compose -f docker/docker-compose-superset.yml down
docker compose -f docker/docker-compose-spark.yml down
docker compose -f docker/docker-compose-hdfs.yml down
```

---

## Implementation Notes

The repository includes an implementation for the Phase 2 pipeline in:

- `processing/analysis.py`
- `visualization/register_tables.py`
- `reports/REPORT.md`

### Run the pipeline

Download the Olist dataset first:

```bash
python scripts/download_dataset.py
```

Run the Spark pipeline locally:

```bash
spark-submit processing/analysis.py --input data/raw --output data/processed
```

Run the Spark pipeline against HDFS from the Docker Spark container:

```bash
docker exec -it spark-master /spark/bin/spark-submit /app/processing/analysis.py \
  --input /app/data/raw \
  --output hdfs://namenode:9000/olist
```

Register the Gold Parquet tables for SQL/Superset access:

```bash
docker exec -it spark-master /spark/bin/spark-submit /app/visualization/register_tables.py \
  --gold-path hdfs://namenode:9000/olist/gold \
  --database olist_gold
```

Validate the generated output folders and row counts:

```bash
docker exec -it spark-master /spark/bin/spark-submit /app/scripts/validate_outputs.py \
  --output hdfs://namenode:9000/olist
```

The pipeline creates:

- Bronze Parquet tables under `bronze/`
- Silver cleaned Parquet tables under `silver/`
- Gold fact/dimension Parquet tables under `gold/`
- Business-question summaries under `reports/`

Use the generated Gold tables to build Superset charts for monthly revenue, category revenue, seller performance, customer-state sales, delivery time, payment trends, and review score by category.
