"""
Small, composable data quality checks for use in the Silver transformation
layer (or anywhere else in the pipeline).

Each check returns a DataQualityResult so callers can decide whether to log,
raise, or route failing rows to a quarantine table.
"""

from dataclasses import dataclass, field
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class DataQualityResult:
    check_name: str
    passed: bool
    details: str = ""
    failing_row_count: int = 0

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.check_name}: {self.details}"


def check_not_null(df: DataFrame, columns: List[str]) -> DataQualityResult:
    """Fail if any of `columns` contain nulls."""
    condition = None
    for c in columns:
        cond = F.col(c).isNull()
        condition = cond if condition is None else (condition | cond)

    failing_count = df.filter(condition).count() if condition is not None else 0
    passed = failing_count == 0
    return DataQualityResult(
        check_name="check_not_null",
        passed=passed,
        details=f"columns={columns}, failing_rows={failing_count}",
        failing_row_count=failing_count,
    )


def check_no_duplicates(df: DataFrame, keys: List[str]) -> DataQualityResult:
    """Fail if there are duplicate rows based on `keys`."""
    total = df.count()
    distinct = df.select(*keys).distinct().count()
    failing_count = total - distinct
    passed = failing_count == 0
    return DataQualityResult(
        check_name="check_no_duplicates",
        passed=passed,
        details=f"keys={keys}, duplicate_rows={failing_count}",
        failing_row_count=failing_count,
    )


def check_value_range(
    df: DataFrame,
    column: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> DataQualityResult:
    """Fail if any value in `column` falls outside [min_value, max_value]."""
    condition = None
    if min_value is not None:
        condition = F.col(column) < min_value
    if max_value is not None:
        upper = F.col(column) > max_value
        condition = upper if condition is None else (condition | upper)

    failing_count = df.filter(condition).count() if condition is not None else 0
    passed = failing_count == 0
    return DataQualityResult(
        check_name="check_value_range",
        passed=passed,
        details=(
            f"column={column}, range=[{min_value}, {max_value}], "
            f"failing_rows={failing_count}"
        ),
        failing_row_count=failing_count,
    )


def check_row_count_above(df: DataFrame, threshold: int) -> DataQualityResult:
    """Fail if the DataFrame has fewer than `threshold` rows."""
    count = df.count()
    passed = count >= threshold
    return DataQualityResult(
        check_name="check_row_count_above",
        passed=passed,
        details=f"threshold={threshold}, actual_count={count}",
        failing_row_count=0 if passed else threshold - count,
    )


def run_checks(results: List[DataQualityResult], raise_on_failure: bool = False) -> bool:
    """Print a summary of all checks; optionally raise if any failed."""
    all_passed = True
    for result in results:
        print(result)
        if not result.passed:
            all_passed = False

    if raise_on_failure and not all_passed:
        raise ValueError("One or more data quality checks failed. See log above.")

    return all_passed
