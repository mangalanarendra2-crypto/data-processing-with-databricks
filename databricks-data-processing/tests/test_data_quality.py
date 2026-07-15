"""
Unit tests for utils/data_quality.py.

Run with: pytest tests/test_data_quality.py
"""

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from pyspark.sql import SparkSession

from utils.data_quality import (
    check_no_duplicates,
    check_not_null,
    check_row_count_above,
    check_value_range,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("test-data-quality").master("local[1]").getOrCreate()
    )
    yield session
    session.stop()


def test_check_not_null_passes(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    result = check_not_null(df, ["id", "name"])
    assert result.passed


def test_check_not_null_fails(spark):
    df = spark.createDataFrame([(1, "a"), (2, None)], ["id", "name"])
    result = check_not_null(df, ["name"])
    assert not result.passed
    assert result.failing_row_count == 1


def test_check_no_duplicates_passes(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    result = check_no_duplicates(df, ["id"])
    assert result.passed


def test_check_no_duplicates_fails(spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], ["id"])
    result = check_no_duplicates(df, ["id"])
    assert not result.passed
    assert result.failing_row_count == 1


def test_check_value_range_passes(spark):
    df = spark.createDataFrame([(5,), (10,)], ["qty"])
    result = check_value_range(df, "qty", min_value=1, max_value=20)
    assert result.passed


def test_check_value_range_fails(spark):
    df = spark.createDataFrame([(5,), (-1,)], ["qty"])
    result = check_value_range(df, "qty", min_value=0)
    assert not result.passed
    assert result.failing_row_count == 1


def test_check_row_count_above_passes(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    result = check_row_count_above(df, 2)
    assert result.passed


def test_check_row_count_above_fails(spark):
    df = spark.createDataFrame([(1,)], ["id"])
    result = check_row_count_above(df, 2)
    assert not result.passed
