# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Aggregation
# MAGIC
# MAGIC Aggregates curated Silver data into business-ready Gold tables for
# MAGIC reporting and BI: daily revenue by region, and top products.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import functions as F  # noqa: E402

from config.config import (  # noqa: E402
    GOLD_REVENUE_BY_REGION_PATH,
    GOLD_REVENUE_BY_REGION_TABLE,
    GOLD_TOP_PRODUCTS_PATH,
    GOLD_TOP_PRODUCTS_TABLE,
    SILVER_PATH,
    USE_UNITY_CATALOG,
)
from utils.spark_helpers import get_spark_session, read_delta, write_delta  # noqa: E402

# COMMAND ----------

spark = get_spark_session()
silver_df = read_delta(spark, SILVER_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 1: Daily revenue by region

# COMMAND ----------

revenue_by_region_df = (
    silver_df.groupBy("order_date", "region")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("order_id").alias("order_count"),
    )
    .orderBy("order_date", "region")
)

write_delta(
    revenue_by_region_df,
    GOLD_REVENUE_BY_REGION_PATH,
    mode="overwrite",
    partition_by=["region"],
)
print(f"Wrote Gold revenue-by-region table to: {GOLD_REVENUE_BY_REGION_PATH}")

if USE_UNITY_CATALOG:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {GOLD_REVENUE_BY_REGION_TABLE} "
        f"USING DELTA LOCATION '{GOLD_REVENUE_BY_REGION_PATH}'"
    )
    print(f"Registered Unity Catalog table: {GOLD_REVENUE_BY_REGION_TABLE}")

try:
    display(revenue_by_region_df.limit(10))  # noqa: F821
except NameError:
    revenue_by_region_df.limit(10).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 2: Top products by revenue

# COMMAND ----------

top_products_df = (
    silver_df.groupBy("product_id", "product_name")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("order_id").alias("order_count"),
    )
    .orderBy(F.col("total_revenue").desc())
)

write_delta(top_products_df, GOLD_TOP_PRODUCTS_PATH, mode="overwrite")
print(f"Wrote Gold top-products table to: {GOLD_TOP_PRODUCTS_PATH}")

if USE_UNITY_CATALOG:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {GOLD_TOP_PRODUCTS_TABLE} "
        f"USING DELTA LOCATION '{GOLD_TOP_PRODUCTS_PATH}'"
    )
    print(f"Registered Unity Catalog table: {GOLD_TOP_PRODUCTS_TABLE}")

try:
    display(top_products_df.limit(10))  # noqa: F821
except NameError:
    top_products_df.limit(10).show(truncate=False)
