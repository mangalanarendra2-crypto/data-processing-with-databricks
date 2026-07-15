# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Transformation
# MAGIC
# MAGIC Cleans, validates, deduplicates, and enforces types on Bronze data,
# MAGIC producing a curated Silver Delta table. Runs data quality checks and
# MAGIC quarantines invalid records instead of silently dropping them.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

from config.config import (  # noqa: E402
    BRONZE_PATH,
    QUARANTINE_PATH,
    SILVER_PATH,
    SILVER_TABLE,
    USE_UNITY_CATALOG,
)
from utils.data_quality import (  # noqa: E402
    check_no_duplicates,
    check_not_null,
    check_value_range,
    run_checks,
)
from utils.spark_helpers import get_spark_session, read_delta, write_delta  # noqa: E402

# COMMAND ----------

spark = get_spark_session()
bronze_df = read_delta(spark, BRONZE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Drop rows missing critical fields, route to quarantine

# COMMAND ----------

critical_columns = ["order_id", "customer_name", "region", "product_id"]

is_valid = F.lit(True)
for c in critical_columns:
    is_valid = is_valid & F.col(c).isNotNull()
is_valid = is_valid & (F.col("quantity") > 0)

valid_df = bronze_df.filter(is_valid)
invalid_df = bronze_df.filter(~is_valid)

print(f"Valid rows: {valid_df.count()} | Quarantined rows: {invalid_df.count()}")

if invalid_df.count() > 0:
    write_delta(invalid_df, QUARANTINE_PATH, mode="overwrite")
    print(f"Quarantined invalid rows written to: {QUARANTINE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Deduplicate on business key (order_id), keeping latest ingest

# COMMAND ----------

window_spec = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())

deduped_df = (
    valid_df.withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

print(f"Rows after deduplication: {deduped_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Enforce types and add derived columns

# COMMAND ----------

silver_df = (
    deduped_df.withColumn("order_date", F.to_date("order_date"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("total_amount", F.round(F.col("unit_price") * F.col("quantity"), 2))
    .withColumn("_processed_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Run data quality checks

# COMMAND ----------

results = [
    check_not_null(silver_df, ["order_id", "customer_name", "region", "product_id"]),
    check_no_duplicates(silver_df, ["order_id"]),
    check_value_range(silver_df, "quantity", min_value=1, max_value=1000),
    check_value_range(silver_df, "total_amount", min_value=0),
]

run_checks(results, raise_on_failure=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write to Silver Delta table

# COMMAND ----------

write_delta(silver_df, SILVER_PATH, mode="overwrite", partition_by=["region"])
print(f"Wrote Silver Delta table to: {SILVER_PATH}")

if USE_UNITY_CATALOG:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_TABLE} USING DELTA LOCATION '{SILVER_PATH}'")
    print(f"Registered Unity Catalog table: {SILVER_TABLE}")

# COMMAND ----------

try:
    display(silver_df.limit(10))  # noqa: F821
except NameError:
    silver_df.limit(10).show(truncate=False)
