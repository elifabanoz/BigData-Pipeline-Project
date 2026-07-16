# Big Data Pipeline Project Report

## Project scope

This project builds an Olist e-commerce data pipeline from nine raw CSV files into a Bronze/Silver/Gold analytical model. The target output is a set of Parquet tables and summary datasets that answer the required business questions:

- Monthly revenue
- Revenue by product category
- Top-performing sellers
- Sales by customer state
- Average delivery time by state
- Payment method trends
- Average review score by category

## Architecture

```text
Olist CSV files
    -> Bronze Parquet tables
    -> Silver cleaned Parquet tables
    -> Gold star schema fact/dimension tables
    -> Superset charts and report summaries
```

### Bronze layer

The Bronze layer stores each raw CSV as Parquet without business transformations. This keeps the original source available for reprocessing.

### Silver layer

The Silver layer applies data type fixes and key data-quality decisions:

- `orders` is deduplicated by `order_id`.
- `order_items` is deduplicated by `order_id + order_item_id`.
- `order_payments` is deduplicated by `order_id + payment_sequential`.
- `geolocation` exact duplicate rows are removed.
- Product category names are translated from Portuguese to English when possible.
- Delivery metrics are calculated only for delivered orders with a delivery timestamp.

### Gold layer

The Gold layer contains reusable fact and dimension tables:

| Table | Grain | Purpose |
|---|---|---|
| `fact_order_payment` | `order_id + payment_sequential` | Monthly revenue and payment trends |
| `fact_order_item_sales` | `order_id + order_item_id` | Category revenue and seller performance |
| `fact_delivery` | `order_id` | Delivery time and delay analysis |
| `fact_review_category` | `review_id + order_id + product_category` | Average review score by category |
| `dim_date` | `date_key` | Time slicing |
| `dim_customer` | `customer_id` | Customer city/state analysis |
| `dim_seller` | `seller_id` | Seller analysis |
| `dim_product` | `product_id` | Product and category analysis |
| `dim_payment_method` | `payment_type` | Payment method analysis |
| `dim_geolocation` | `zip/city/state` | Location lookup |

## Revenue definitions

The report separates revenue metrics to avoid mixing incompatible totals:

- **Payment revenue:** `SUM(payment_value)` from `order_payments`. Used for monthly revenue, sales by customer state, and payment trends.
- **Product GMV:** `SUM(price)` from `order_items`. Used for product category and seller gross sales.
- **Freight:** `SUM(freight_value)`, kept separately.
- **Item total value:** `SUM(price + freight_value)`, useful as an operational total but not treated as pure product revenue.

## Review modeling note

The Olist review table is at order level and does not directly contain `product_id`. Joining reviews to order items can multiply one review across multiple products. To reduce this risk, the Gold model uses `fact_review_category` with grain:

```text
review_id + order_id + product_category
```

The same order-category combination is deduplicated before category-level review averages are calculated.

## How to run

### 1. Download the dataset

```bash
python scripts/download_dataset.py
```

This creates `data/raw` with the nine Olist CSV files.

### 2. Run the Spark pipeline locally

```bash
spark-submit processing/analysis.py --input data/raw --output data/processed
```

### 3. Run with HDFS in Docker

```bash
bash scripts/setup_network.sh
# or on Windows PowerShell:
# .\scripts\setup_network.ps1

docker compose -f docker/docker-compose-hdfs.yml up -d
docker compose -f docker/docker-compose-spark.yml up -d

docker exec -it spark-master /spark/bin/spark-submit /app/processing/analysis.py \
  --input /app/data/raw \
  --output hdfs://namenode:9000/olist
```

### 4. Register Gold tables for Superset

```bash
docker exec -it spark-master /spark/bin/spark-submit /app/visualization/register_tables.py \
  --gold-path hdfs://namenode:9000/olist/gold \
  --database olist_gold
```

### 5. Validate generated outputs

```bash
docker exec -it spark-master /spark/bin/spark-submit /app/scripts/validate_outputs.py \
  --output hdfs://namenode:9000/olist
```

Validation result from the completed run:

- Bronze tables validated: 9
- Silver tables validated: 9
- Gold tables validated: 10
- Report summaries validated: 7
- Geolocation cleanup validated: `1,000,163 -> 738,332` rows

Superset connection suggestion:

```text
hive://spark-thriftserver:10000/olist_gold
```

## Expected evidence to capture

For final submission, add screenshots or command outputs for:

- Spark job completion output
- Output validation script completion output
- Bronze/Silver/Gold Parquet folders
- Geolocation cleanup: `1,000,163 -> 738,332` rows after exact duplicate removal
- HDFS NameNode or MinIO bucket file listing
- Superset dashboard overview
- Monthly revenue chart
- Revenue by product category chart
- Sales by customer state chart or map
- Top-performing sellers chart
- Delivery time by state chart
- Payment method trend chart
- Average review score by category chart

## Business question outputs

The pipeline writes CSV summaries under:

```text
data/processed/reports/
```

Generated report folders:

- `monthly_revenue`
- `revenue_by_product_category`
- `top_performing_sellers`
- `sales_by_customer_state`
- `average_delivery_time_by_state`
- `payment_method_trends`
- `average_review_score_by_category`

## Submission checklist

- [ ] Fork the repository
- [ ] Create a working branch
- [ ] Run the dataset download
- [ ] Run the Spark pipeline
- [ ] Register Gold tables
- [ ] Create Superset charts
- [ ] Add screenshots to the final LMS report
- [ ] Commit and push the branch
- [ ] Open a Pull Request to the upstream repository
- [ ] Add fork, branch, commit hash, and PR link to the LMS report
