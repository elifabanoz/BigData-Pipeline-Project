"""Validate generated Olist pipeline outputs.

Run examples
------------
Local Spark:
    spark-submit scripts/validate_outputs.py --output data/processed

Docker Spark master container:
    docker exec -it spark-master /spark/bin/spark-submit /app/scripts/validate_outputs.py \
        --output hdfs://namenode:9000/olist
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

from pyspark.sql import SparkSession


EXPECTED_BRONZE_TABLES = [
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "customers",
    "sellers",
    "products",
    "geolocation",
    "category_translation",
]

EXPECTED_SILVER_TABLES = EXPECTED_BRONZE_TABLES

EXPECTED_GOLD_TABLES = [
    "dim_date",
    "dim_customer",
    "dim_seller",
    "dim_product",
    "dim_payment_method",
    "dim_geolocation",
    "fact_order_payment",
    "fact_order_item_sales",
    "fact_delivery",
    "fact_review_category",
]

EXPECTED_REPORTS = [
    "monthly_revenue",
    "revenue_by_product_category",
    "top_performing_sellers",
    "sales_by_customer_state",
    "average_delivery_time_by_state",
    "payment_method_trends",
    "average_review_score_by_category",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Olist pipeline output folders")
    parser.add_argument("--output", default="data/processed", help="Pipeline output root")
    parser.add_argument("--app-name", default="olist-validate-pipeline-outputs", help="Spark application name")
    return parser.parse_args()


def join_path(root: str, *parts: str) -> str:
    if "://" in root:
        return "/".join([root.rstrip("/"), *parts])
    return str(Path(root, *parts))


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).config("spark.sql.session.timeZone", "UTC").getOrCreate()


def count_parquet_table(spark: SparkSession, path: str) -> int:
    return spark.read.parquet(path).count()


def count_csv_report(spark: SparkSession, path: str) -> int:
    return spark.read.option("header", "true").csv(path).count()


def validate_group(
    spark: SparkSession,
    output_root: str,
    layer: str,
    names: List[str],
    format_name: str,
) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for name in names:
        path = join_path(output_root, layer, name)
        if format_name == "parquet":
            row_count = count_parquet_table(spark, path)
        else:
            row_count = count_csv_report(spark, path)
        if row_count <= 0:
            raise ValueError(f"{layer}/{name} is empty")
        counts[f"{layer}/{name}"] = row_count
    return counts


def main() -> None:
    args = parse_args()
    spark = build_spark(args.app_name)
    spark.sparkContext.setLogLevel("WARN")

    validation_counts: Dict[str, int] = {}
    validation_counts.update(validate_group(spark, args.output, "bronze", EXPECTED_BRONZE_TABLES, "parquet"))
    validation_counts.update(validate_group(spark, args.output, "silver", EXPECTED_SILVER_TABLES, "parquet"))
    validation_counts.update(validate_group(spark, args.output, "gold", EXPECTED_GOLD_TABLES, "parquet"))
    validation_counts.update(validate_group(spark, args.output, "reports", EXPECTED_REPORTS, "csv"))

    print("Output validation completed successfully")
    print(f"Bronze tables validated: {len(EXPECTED_BRONZE_TABLES)}")
    print(f"Silver tables validated: {len(EXPECTED_SILVER_TABLES)}")
    print(f"Gold tables validated: {len(EXPECTED_GOLD_TABLES)}")
    print(f"Report summaries validated: {len(EXPECTED_REPORTS)}")
    print(f"Output root: {args.output}")

    for name in sorted(validation_counts):
        print(f"{name}: {validation_counts[name]:,} rows")

    spark.stop()


if __name__ == "__main__":
    main()
