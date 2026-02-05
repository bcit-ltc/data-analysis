"""Structural and schema profiling utilities.

This module defines operations for:
- cleaning and standardizing column names
- computing basic structural metrics (rows, columns)
- summarizing sparsity/missingness to aid initial data understanding

These helpers are designed to work with PySpark ``DataFrame`` objects so they
can be called from any Spark-based ETL script in this project.
"""

import re
from typing import Iterable, List, Mapping, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# Default set of string sentinel values that will be treated as "missing" when
# encountered in string-typed columns (in addition to NULL and empty strings).
_DEFAULT_MISSING_SENTINELS = (
    "n/a",
    "na",
    "n.a.",
    "none",
    "null",
    "unknown",
    "unspecified",
    "missing",
)


def standardize_column_names(
    columns: Iterable[str],
    *,
    case: str = "snake",
    strip_whitespace: bool = True,
) -> List[str]:
    """Return standardized versions of column names.

    Parameters
    ----------
    columns: Original column names.
    case: Naming style to target (e.g., ``"snake"``, ``"lower"``, ``"upper"``).
    strip_whitespace: Whether to trim leading/trailing whitespace.
    """
    normalised: List[str] = []

    for col in columns:
        name = str(col)

        if strip_whitespace:
            name = name.strip()

        if case == "lower":
            name = name.lower()
        elif case == "upper":
            name = name.upper()
        elif case == "snake":
            # Collapse internal whitespace to single spaces first.
            name = re.sub(r"\s+", " ", name)

            # Handle CamelCase / PascalCase boundaries.
            name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
            name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)

            # Replace any remaining non-word characters with underscores.
            name = re.sub(r"[^0-9a-zA-Z_]+", "_", name)

            # Normalise to lowercase snake_case and strip redundant underscores.
            name = name.lower()
            name = re.sub(r"_+", "_", name).strip("_")
        else:
            raise ValueError(f"Unsupported case style: {case}")

        normalised.append(name)

    return normalised


def profile_shape(df: DataFrame) -> Mapping[str, int]:
    """Return basic structural information for a DataFrame.

    Includes row and column counts. This intentionally performs a full
    ``count()`` on the input ``DataFrame``.
    """
    n_rows = df.count()
    n_columns = len(df.columns)
    return {"n_rows": int(n_rows), "n_columns": int(n_columns)}


def profile_missingness(
    df: DataFrame,
    *,
    max_columns: Optional[int] = None,
) -> DataFrame:
    """Summarize sparsity/missingness per column.

    The result is a small Spark ``DataFrame`` with one row per input column and
    the following schema::

        column string, n_missing long, pct_missing double

    Parameters
    ----------
    df:
        Input Spark ``DataFrame`` to profile.
    max_columns:
        Optional limit on the number of columns to include (useful for very
        wide tables). Columns are taken in their original order.
    """
    total_rows = df.count()

    # Short-circuit for empty DataFrames.
    if total_rows == 0 or not df.columns:
        return df.sparkSession.createDataFrame([], schema="column string, n_missing long, pct_missing double")

    cols = list(df.columns)
    if max_columns is not None:
        cols = cols[: max_columns]

    schema_by_name = {field.name: field.dataType for field in df.schema}

    # Compute missing-value counts for the selected columns in a single pass.
    # For string-typed columns we treat NULLs, empty strings, and a small set
    # of common sentinel values (e.g., "N/A", "unknown") as missing.
    agg_exprs = []
    for c in cols:
        col_expr = F.col(c)
        cond = col_expr.isNull()

        data_type = schema_by_name.get(c)
        if isinstance(data_type, StringType):
            lowered_trimmed = F.lower(F.trim(col_expr))
            cond = (
                cond
                | (lowered_trimmed == "")
                | lowered_trimmed.isin(*_DEFAULT_MISSING_SENTINELS)
            )

        agg_exprs.append(F.sum(F.when(cond, 1).otherwise(0)).alias(c))
    missing_row = df.agg(*agg_exprs).collect()[0].asDict()

    summary_rows = []
    for c in cols:
        n_missing = int(missing_row.get(c, 0) or 0)
        pct_missing = (n_missing / total_rows) if total_rows > 0 else None
        summary_rows.append(
            (c, n_missing, float(pct_missing) if pct_missing is not None else None)
        )

    return df.sparkSession.createDataFrame(
        summary_rows,
        schema="column string, n_missing long, pct_missing double",
    )
