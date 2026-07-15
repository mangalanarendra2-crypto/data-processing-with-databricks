# Data Processing with Databricks

A simple base project demonstrating an end-to-end data processing pipeline on
Databricks using PySpark and Delta Lake, following the **Medallion Architecture**
(Bronze → Silver → Gold).

## Architecture

```
Raw Source Data
      │
      ▼
 ┌──────────┐   Ingest raw files as-is, add metadata
 │  BRONZE  │   (schema-on-read, minimal transformation)
 └────┬─────┘
      ▼
 ┌──────────┐   Clean, validate, deduplicate, enforce schema
 │  SILVER  │
 └────┬─────┘
      ▼
 ┌──────────┐   Business-level aggregates, ready for BI/analytics
 │   GOLD   │
 └──────────┘
```

## Project Structure

```
databricks-data-processing/
├── README.md
├── requirements.txt
├── config/
│   └── config.py              # Central configuration (paths, table names)
├── notebooks/
│   ├── 00_generate_sample_data.py   # Creates sample raw sales data
│   ├── 01_bronze_ingestion.py       # Raw ingestion layer
│   ├── 02_silver_transformation.py  # Cleaning & transformation layer
│   └── 03_gold_aggregation.py       # Business aggregation layer
├── utils/
│   ├── __init__.py
│   ├── data_quality.py        # Reusable data quality check functions
│   └── spark_helpers.py       # Reusable Spark session / IO helpers
├── tests/
│   └── test_data_quality.py   # Unit tests for data quality functions
└── data/                       # Local sample data (for non-Databricks runs)
```

## What the pipeline does

The example use case is **retail sales order processing**:

1. **Bronze (`01_bronze_ingestion.py`)** — Reads raw CSV/JSON order data, attaches
   ingestion metadata (`_ingested_at`, `_source_file`), and writes it untouched
   into a Bronze Delta table.
2. **Silver (`02_silver_transformation.py`)** — Cleans nulls, deduplicates,
   fixes types, filters invalid records, and writes a curated Silver Delta
   table. Runs data quality checks along the way.
3. **Gold (`03_gold_aggregation.py`)** — Aggregates Silver data into daily
   revenue-by-region and top-products summary tables for reporting/BI.

## Running on Databricks

1. Import this repo into a Databricks Repo (Repos → Add Repo).
2. Attach each notebook under `notebooks/` to a cluster (DBR 13.x+ recommended,
   Unity Catalog or Delta Lake enabled).
3. Update paths/catalog names in `config/config.py` to match your workspace.
4. Run notebooks in order: `00` → `01` → `02` → `03`.

## Running locally (for testing, using PySpark + local Delta)

```bash
pip install -r requirements.txt
python notebooks/00_generate_sample_data.py
python notebooks/01_bronze_ingestion.py
python notebooks/02_silver_transformation.py
python notebooks/03_gold_aggregation.py
```

## Data Quality Checks

`utils/data_quality.py` provides small, composable checks used in the Silver
layer:

- `check_not_null(df, columns)`
- `check_no_duplicates(df, keys)`
- `check_value_range(df, column, min_value, max_value)`
- `check_row_count_above(df, threshold)`

Each returns a `DataQualityResult` (pass/fail + details) so checks can be
logged, asserted, or routed to a quarantine table.

## Extending this base project

- Swap the sample CSV/JSON source for Auto Loader (`cloudFiles`) to handle
  streaming ingestion.
- Add a `job.yml` / Databricks Workflow to schedule the three notebooks in
  sequence.
- Add `expectations` via Delta Live Tables (DLT) if you want declarative
  pipelines instead of notebook orchestration.
- Point Gold tables at a BI tool (e.g. Databricks SQL dashboards, Power BI).
