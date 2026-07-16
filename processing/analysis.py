"""Olist big data pipeline: Bronze, Silver and Gold transformations.

Run examples
------------
Local Spark:
    spark-submit processing/analysis.py --input data/raw --output data/processed

Docker Spark master container:
    docker exec -it spark-master spark-submit /app/processing/analysis.py \
        --input /app/data/raw \
        --output hdfs://namenode:9000/olist

The script reads the nine Olist CSV files, writes Bronze/Silver/Gold Parquet
outputs, and exports CSV summaries for the business questions.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Iterable, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


RAW_TABLES: Dict[str, str] = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

TIMESTAMP_COLUMNS: Dict[str, Iterable[str]] = {
    "orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "order_items": ["shipping_limit_date"],
    "order_reviews": ["review_creation_date", "review_answer_timestamp"],
}

NUMERIC_COLUMNS: Dict[str, Iterable[Tuple[str, T.DataType]]] = {
    "order_items": [
        ("order_item_id", T.IntegerType()),
        ("price", T.DoubleType()),
        ("freight_value", T.DoubleType()),
    ],
    "order_payments": [
        ("payment_sequential", T.IntegerType()),
        ("payment_installments", T.IntegerType()),
        ("payment_value", T.DoubleType()),
    ],
    "order_reviews": [("review_score", T.IntegerType())],
    "products": [
        ("product_name_lenght", T.IntegerType()),
        ("product_description_lenght", T.IntegerType()),
        ("product_photos_qty", T.IntegerType()),
        ("product_weight_g", T.DoubleType()),
        ("product_length_cm", T.DoubleType()),
        ("product_height_cm", T.DoubleType()),
        ("product_width_cm", T.DoubleType()),
    ],
    "geolocation": [
        ("geolocation_zip_code_prefix", T.IntegerType()),
        ("geolocation_lat", T.DoubleType()),
        ("geolocation_lng", T.DoubleType()),
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Olist Bronze/Silver/Gold data pipeline")
    parser.add_argument("--input", default="data/raw", help="Folder containing the nine Olist CSV files")
    parser.add_argument("--output", default="data/processed", help="Output root for Parquet and report CSV files")
    parser.add_argument("--app-name", default="olist-big-data-pipeline", help="Spark application name")
    parser.add_argument("--s3-endpoint", default=None, help="Optional MinIO/S3 endpoint, for example http://minio:9000")
    parser.add_argument("--s3-access-key", default=None, help="Optional MinIO/S3 access key")
    parser.add_argument("--s3-secret-key", default=None, help="Optional MinIO/S3 secret key")
    return parser.parse_args()


def build_spark(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if args.s3_endpoint:
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    if args.s3_access_key:
        builder = builder.config("spark.hadoop.fs.s3a.access.key", args.s3_access_key)
    if args.s3_secret_key:
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key)
    return builder.getOrCreate()


def join_path(root: str, *parts: str) -> str:
    if "://" in root:
        return "/".join([root.rstrip("/"), *parts])
    return str(Path(root, *parts))


def load_csv(spark: SparkSession, input_root: str, table_name: str, file_name: str) -> DataFrame:
    path = join_path(input_root, file_name)
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    for column in TIMESTAMP_COLUMNS.get(table_name, []):
        if column in df.columns:
            df = df.withColumn(column, F.to_timestamp(F.col(column)))
    for column, data_type in NUMERIC_COLUMNS.get(table_name, []):
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast(data_type))
    return df


def write_parquet(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").parquet(path)


def write_csv(df: DataFrame, path: str) -> None:
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)


def build_date_dim(orders: DataFrame) -> DataFrame:
    bounds = orders.select(
        F.to_date(F.min("order_purchase_timestamp")).alias("min_date"),
        F.to_date(F.max("order_purchase_timestamp")).alias("max_date"),
    )
    return (
        bounds.select(F.explode(F.sequence("min_date", "max_date", F.expr("interval 1 day"))).alias("date"))
        .select(
            F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("date").alias("calendar_date"),
            F.year("date").alias("year"),
            F.quarter("date").alias("quarter"),
            F.month("date").alias("month"),
            F.date_format("date", "yyyy-MM").alias("year_month"),
            F.weekofyear("date").alias("week_of_year"),
        )
        .dropDuplicates(["date_key"])
    )


def main() -> None:
    args = parse_args()
    spark = build_spark(args)
    spark.sparkContext.setLogLevel("WARN")

    raw = {name: load_csv(spark, args.input, name, file_name) for name, file_name in RAW_TABLES.items()}

    bronze_root = join_path(args.output, "bronze")
    silver_root = join_path(args.output, "silver")
    gold_root = join_path(args.output, "gold")
    report_root = join_path(args.output, "reports")

    for name, df in raw.items():
        write_parquet(df, join_path(bronze_root, name))

    silver = {
        "orders": raw["orders"].dropDuplicates(["order_id"]),
        "order_items": raw["order_items"].dropDuplicates(["order_id", "order_item_id"]),
        "order_payments": raw["order_payments"].dropDuplicates(["order_id", "payment_sequential"]),
        "order_reviews": raw["order_reviews"].dropDuplicates(["review_id", "order_id"]),
        "customers": raw["customers"].dropDuplicates(["customer_id"]),
        "sellers": raw["sellers"].dropDuplicates(["seller_id"]),
        "products": raw["products"].dropDuplicates(["product_id"]),
        "geolocation": raw["geolocation"].dropDuplicates(),
        "category_translation": raw["category_translation"].dropDuplicates(["product_category_name"]),
    }

    for name, df in silver.items():
        write_parquet(df, join_path(silver_root, name))

    orders = silver["orders"]
    customers = silver["customers"]
    sellers = silver["sellers"]
    products = silver["products"]
    translation = silver["category_translation"]
    items = silver["order_items"]
    payments = silver["order_payments"]
    reviews = silver["order_reviews"]

    product_dim = (
        products.join(translation, "product_category_name", "left")
        .withColumn("product_category_name_english", F.coalesce("product_category_name_english", "product_category_name", F.lit("unknown")))
        .select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        )
    )

    dim_customer = customers.select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    )
    dim_seller = sellers.select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
    dim_date = build_date_dim(orders)
    dim_payment_method = payments.select("payment_type").dropDuplicates().withColumn("payment_method_key", F.monotonically_increasing_id())
    dim_geolocation = (
        silver["geolocation"]
        .groupBy("geolocation_zip_code_prefix", "geolocation_city", "geolocation_state")
        .agg(F.avg("geolocation_lat").alias("avg_lat"), F.avg("geolocation_lng").alias("avg_lng"), F.count("*").alias("source_rows"))
    )

    fact_order_payment = (
        payments.join(orders.select("order_id", "customer_id", "order_purchase_timestamp", "order_status"), "order_id", "left")
        .select(
            "order_id",
            "payment_sequential",
            "customer_id",
            F.to_date("order_purchase_timestamp").alias("purchase_date"),
            F.date_format("order_purchase_timestamp", "yyyy-MM").alias("purchase_month"),
            "order_status",
            "payment_type",
            "payment_installments",
            "payment_value",
        )
    )

    fact_order_item_sales = (
        items.join(orders.select("order_id", "customer_id", "order_purchase_timestamp", "order_status"), "order_id", "left")
        .join(product_dim.select("product_id", "product_category_name_english"), "product_id", "left")
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "customer_id",
            F.to_date("order_purchase_timestamp").alias("purchase_date"),
            F.date_format("order_purchase_timestamp", "yyyy-MM").alias("purchase_month"),
            "order_status",
            F.coalesce("product_category_name_english", F.lit("unknown")).alias("product_category"),
            F.col("price").alias("product_gmv"),
            "freight_value",
            (F.col("price") + F.col("freight_value")).alias("item_total_value"),
        )
    )

    fact_delivery = (
        orders.join(customers.select("customer_id", "customer_state"), "customer_id", "left")
        .where((F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNotNull())
        .select(
            "order_id",
            "customer_id",
            "customer_state",
            F.to_date("order_purchase_timestamp").alias("purchase_date"),
            F.to_date("order_delivered_customer_date").alias("delivered_date"),
            F.to_date("order_estimated_delivery_date").alias("estimated_delivery_date"),
            F.datediff("order_delivered_customer_date", "order_purchase_timestamp").alias("delivery_days"),
            F.datediff("order_delivered_customer_date", "order_estimated_delivery_date").alias("delay_days"),
        )
    )

    order_category = fact_order_item_sales.select("order_id", "product_category").dropDuplicates()
    fact_review_category = (
        reviews.join(order_category, "order_id", "left")
        .select(
            "review_id",
            "order_id",
            F.coalesce("product_category", F.lit("unknown")).alias("product_category"),
            "review_score",
            F.to_date("review_creation_date").alias("review_date"),
            F.to_date("review_answer_timestamp").alias("review_answer_date"),
        )
        .dropDuplicates(["review_id", "order_id", "product_category"])
    )

    gold_tables = {
        "dim_date": dim_date,
        "dim_customer": dim_customer,
        "dim_seller": dim_seller,
        "dim_product": product_dim,
        "dim_payment_method": dim_payment_method,
        "dim_geolocation": dim_geolocation,
        "fact_order_payment": fact_order_payment,
        "fact_order_item_sales": fact_order_item_sales,
        "fact_delivery": fact_delivery,
        "fact_review_category": fact_review_category,
    }

    for name, df in gold_tables.items():
        write_parquet(df, join_path(gold_root, name))

    summaries = {
        "monthly_revenue": fact_order_payment.groupBy("purchase_month").agg(F.sum("payment_value").alias("payment_revenue")).orderBy("purchase_month"),
        "revenue_by_product_category": fact_order_item_sales.groupBy("product_category").agg(
            F.count("order_item_id").alias("order_items"),
            F.sum("product_gmv").alias("product_gmv"),
            F.sum("freight_value").alias("freight"),
            F.sum("item_total_value").alias("item_total_value"),
        ).orderBy(F.desc("product_gmv")),
        "top_performing_sellers": fact_order_item_sales.groupBy("seller_id").agg(
            F.countDistinct("order_id").alias("orders"),
            F.count("order_item_id").alias("order_items"),
            F.sum("product_gmv").alias("gross_sales"),
        ).orderBy(F.desc("gross_sales")),
        "sales_by_customer_state": fact_order_payment.join(dim_customer.select("customer_id", "customer_state"), "customer_id", "left").groupBy("customer_state").agg(
            F.count("payment_value").alias("payment_rows"),
            F.sum("payment_value").alias("payment_revenue"),
        ).orderBy(F.desc("payment_revenue")),
        "average_delivery_time_by_state": fact_delivery.groupBy("customer_state").agg(
            F.count("order_id").alias("delivered_orders"),
            F.avg("delivery_days").alias("avg_delivery_days"),
        ).orderBy(F.desc("avg_delivery_days")),
        "payment_method_trends": fact_order_payment.groupBy("purchase_month", "payment_type").agg(
            F.count("order_id").alias("payment_rows"),
            F.sum("payment_value").alias("payment_revenue"),
        ).orderBy("purchase_month", "payment_type"),
        "average_review_score_by_category": fact_review_category.groupBy("product_category").agg(
            F.count("review_id").alias("review_rows"),
            F.avg("review_score").alias("avg_review_score"),
        ).orderBy(F.desc("avg_review_score")),
    }

    for name, df in summaries.items():
        write_csv(df, join_path(report_root, name))

    raw_geo_rows = raw["geolocation"].count()
    silver_geo_rows = silver["geolocation"].count()
    print("Pipeline completed successfully")
    print(f"Bronze tables: {len(raw)}")
    print(f"Silver geolocation rows: {raw_geo_rows:,} -> {silver_geo_rows:,}")
    print(f"Gold tables: {len(gold_tables)}")
    print(f"Report summaries: {len(summaries)}")
    print(f"Output root: {args.output}")

    spark.stop()


if __name__ == "__main__":
    main()
