"""
Central configuration for the Data Processing with Databricks base project.

Update BASE_PATH (and catalog/schema names, if using Unity Catalog) to match
your environment before running the notebooks.
"""

import os

# ---------------------------------------------------------------------------
# Base paths
# ---------------------------------------------------------------------------
# On Databricks, point this at a DBFS or Volumes path, e.g.:
#   "/Volumes/main/data_processing_demo/files"
#   "dbfs:/mnt/data_processing_demo"
# When running locally, this defaults to a folder inside the project.
BASE_PATH = os.environ.get("BASE_PATH", os.path.join(os.getcwd(), "data"))

RAW_DATA_PATH = f"{BASE_PATH}/raw/orders"
BRONZE_PATH = f"{BASE_PATH}/bronze/orders"
SILVER_PATH = f"{BASE_PATH}/silver/orders"
GOLD_REVENUE_BY_REGION_PATH = f"{BASE_PATH}/gold/revenue_by_region"
GOLD_TOP_PRODUCTS_PATH = f"{BASE_PATH}/gold/top_products"
QUARANTINE_PATH = f"{BASE_PATH}/quarantine/orders"

# ---------------------------------------------------------------------------
# Unity Catalog table names (optional — used if you register Delta tables
# instead of / in addition to writing to paths)
# ---------------------------------------------------------------------------
CATALOG = os.environ.get("CATALOG", "main")
SCHEMA = os.environ.get("SCHEMA", "data_processing_demo")

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_orders"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_orders"
GOLD_REVENUE_BY_REGION_TABLE = f"{CATALOG}.{SCHEMA}.gold_revenue_by_region"
GOLD_TOP_PRODUCTS_TABLE = f"{CATALOG}.{SCHEMA}.gold_top_products"

# ---------------------------------------------------------------------------
# Sample data generation settings
# ---------------------------------------------------------------------------
SAMPLE_NUM_ORDERS = 5_000
SAMPLE_SEED = 42

# Whether to also register tables in Unity Catalog / the metastore.
# Set to False when running locally without Unity Catalog access.
USE_UNITY_CATALOG = os.environ.get("USE_UNITY_CATALOG", "false").lower() == "true"
