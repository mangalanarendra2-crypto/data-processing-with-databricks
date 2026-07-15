"""
Reusable Spark session and I/O helpers.

On Databricks, a SparkSession (`spark`) is already provided in the notebook
environment, so `get_spark_session()` simply returns it if it exists.
When running locally, it builds a local Spark session with Delta Lake
support enabled.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "data-processing-with-databricks") -> SparkSession:
    """
    Return the active SparkSession.

    On Databricks, reuses the notebook's existing `spark` session.
    Locally, builds a new session configured for Delta Lake.
    """
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:
        # delta-spark not installed; fall back to plain Spark (Parquet I/O
        # will still work, but Delta-specific operations will fail).
        pass

    return builder.getOrCreate()


def write_delta(df, path: str, mode: str = "overwrite", partition_by=None) -> None:
    """Write a DataFrame to a Delta table path."""
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def read_delta(spark: SparkSession, path: str):
    """Read a Delta table from a path."""
    return spark.read.format("delta").load(path)
