"""Register Olist Gold Parquet tables for SQL tools such as Superset.

Run examples
------------
Local Spark:
    spark-submit visualization/register_tables.py --gold-path data/processed/gold

Docker Spark Thrift/Spark environment:
    docker exec -it spark-master spark-submit /app/visualization/register_tables.py \
        --gold-path hdfs://namenode:9000/olist/gold \
        --database olist_gold
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict

from pyspark.sql import SparkSession


GOLD_TABLES = [
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register Olist Gold Parquet tables")
    parser.add_argument("--gold-path", default="data/processed/gold", help="Gold Parquet root path")
    parser.add_argument("--database", default="olist_gold", help="Spark SQL database name")
    parser.add_argument("--mode", choices=["external", "managed"], default="external", help="Register external tables or save managed Hive tables")
    return parser.parse_args()


def join_path(root: str, table: str) -> str:
    if "://" in root:
        return f"{root.rstrip('/')}/{table}"
    return str(Path(root, table)).replace("\\", "/")


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("olist-register-gold-tables").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.database}")
    spark.sql(f"USE {args.database}")

    for table in GOLD_TABLES:
        path = join_path(args.gold_path, table)
        df = spark.read.parquet(path)
        if args.mode == "managed":
            df.write.mode("overwrite").saveAsTable(table)
        else:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            spark.sql(f"CREATE TABLE {table} USING PARQUET LOCATION '{path}'")
        print(f"Registered {args.database}.{table} from {path}")

    print("\nSuperset connection suggestion:")
    print("SQLAlchemy URI: hive://spark-thriftserver:10000/olist_gold")
    print("Then create charts from the olist_gold fact and dimension tables.")
    spark.stop()


if __name__ == "__main__":
    main()
