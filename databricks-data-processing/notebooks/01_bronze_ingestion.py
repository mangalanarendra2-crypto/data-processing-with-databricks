# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingestion
# MAGIC
# MAGIC Reads raw order data as-is (schema-on-read), attaches ingestion metadata,
# MAGIC and writes it untouched into a Bronze Delta table. No business logic or
# MAGIC cleaning happens at this layer — it's the immutable "raw" record.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import functions as F  # noqa: E402

from config.config import (  # noqa: E402
    BRONZE_PATH,
    BRONZE_TABLE,
    RAW_DATA_PATH,
    USE_UNITY_CATALOG,
)
from utils.spark_helpers import get_spark_session, write_delta  # noqa: E402

# COMMAND ----------

spark = get_spark_session()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read raw source data
# MAGIC In production, replace this with Auto Loader for incremental/streaming
# MAGIC ingestion, e.g.:
# MAGIC ```python
# MAGIC df = (spark.readStream.format("cloudFiles")
# MAGIC       .option("cloudFiles.format", "csv")
# MAGIC       .option("header", "true")
# MAGIC       .load(RAW_DATA_PATH))
# MAGIC ```

# COMMAND ----------

raw_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(RAW_DATA_PATH)
)

print(f"Read {raw_df.count()} raw rows from {RAW_DATA_PATH}")
raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attach ingestion metadata
# MAGIC Bronze keeps every column from the source and adds lineage/audit columns.

# COMMAND ----------

bronze_df = raw_df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
    "_source_file", F.input_file_name()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Delta table

# COMMAND ----------

write_delta(bronze_df, BRONZE_PATH, mode="overwrite")
print(f"Wrote Bronze Delta table to: {BRONZE_PATH}")

if USE_UNITY_CATALOG:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} USING DELTA LOCATION '{BRONZE_PATH}'")
    print(f"Registered Unity Catalog table: {BRONZE_TABLE}")

# COMMAND ----------

try:
    display(bronze_df.limit(10))  # noqa: F821  (display is a Databricks builtin)
except NameError:
    bronze_df.limit(10).show(truncate=False)
