"""Structural and schema profiling utilities.

This module defines operations for:
- cleaning and standardizing column names
- computing basic structural metrics (rows, columns)
- summarizing sparsity/missingness to aid initial data understanding

These helpers are designed to work with PySpark ``DataFrame`` objects so they
can be called from any Spark-based ETL script in this project.
"""

import os
import re
from typing import Iterable, List, Mapping, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, NumericType


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


def _format_spark_table(df: DataFrame) -> str:
    cols = df.columns
    if not cols:
        return "(no columns)"

    rows = [row.asDict(recursive=True) for row in df.collect()]
    if not rows:
        return " | ".join(cols)

    col_widths = []
    for c in cols:
        max_len = max(len(str(r.get(c, ""))) for r in rows)
        col_widths.append(max(len(c), max_len))

    header = " | ".join(c.ljust(w) for c, w in zip(cols, col_widths))
    sep = "-+-".join("-" * w for w in col_widths)
    lines = [header, sep]

    for r in rows:
        line = " | ".join(str(r.get(c, "")).ljust(w) for c, w in zip(cols, col_widths))
        lines.append(line)

    return "\n".join(lines)


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


def profile_cardinality(
    df: DataFrame,
    *,
    max_columns: Optional[int] = None,
    use_approx: bool = True,
) -> DataFrame:
    """Compute per-column cardinality (distinct value counts).

    Returns a small Spark ``DataFrame`` with schema::

        column string, n_distinct long, pct_distinct double

    ``n_distinct`` is the number of distinct *non-null* values in the column.
    ``pct_distinct`` is ``n_distinct / n_rows``.
    """

    total_rows = df.count()

    if total_rows == 0 or not df.columns:
        return df.sparkSession.createDataFrame([], schema="column string, n_distinct long, pct_distinct double")

    cols = list(df.columns)
    if max_columns is not None:
        cols = cols[: max_columns]

    if use_approx:
        agg_exprs = [
            F.approx_count_distinct(F.col(c)).alias(c)
            for c in cols
        ]
    else:
        agg_exprs = [
            F.countDistinct(F.col(c)).alias(c)
            for c in cols
        ]

    cardinality_row = df.agg(*agg_exprs).collect()[0].asDict()

    summary_rows = []
    for c in cols:
        n_distinct = int(cardinality_row.get(c, 0) or 0)
        pct_distinct = (n_distinct / total_rows) if total_rows > 0 else None
        summary_rows.append(
            (c, n_distinct, float(pct_distinct) if pct_distinct is not None else None)
        )

    return df.sparkSession.createDataFrame(
        summary_rows,
        schema="column string, n_distinct long, pct_distinct double",
    )


def profile_top_values(
    df: DataFrame,
    *,
    k: int = 5,
    max_columns: Optional[int] = None,
) -> DataFrame:
    """Return the most common values per column.

    This function is intended for exploratory, informational use. For each
    column it returns up to ``k`` of the most frequent *non-missing* values,
    where "missing" follows the same rules as :func:`profile_missingness`.

    The result is a small Spark ``DataFrame`` with schema::

        column string, value string, count long, pct double

    where ``pct`` is ``count / n_rows``.
    """

    if k <= 0:
        raise ValueError("k must be positive")

    total_rows = df.count()

    if total_rows == 0 or not df.columns:
        return df.sparkSession.createDataFrame([], schema="column string, value string, count long, pct double")

    cols = list(df.columns)
    if max_columns is not None:
        cols = cols[: max_columns]

    schema_by_name = {field.name: field.dataType for field in df.schema}

    summary_rows = []

    for c in cols:
        col_expr = F.col(c)
        data_type = schema_by_name.get(c)

        # Reuse the same missing-value semantics as ``profile_missingness``.
        cond_missing = col_expr.isNull()
        if isinstance(data_type, StringType):
            lowered_trimmed = F.lower(F.trim(col_expr))
            cond_missing = (
                cond_missing
                | (lowered_trimmed == "")
                | lowered_trimmed.isin(*_DEFAULT_MISSING_SENTINELS)
            )

        non_missing_df = df.where(~cond_missing)

        if non_missing_df.rdd.isEmpty():  # no non-missing values for this column
            continue

        top_k = (
            non_missing_df
            .groupBy(col_expr)
            .agg(F.count(F.lit(1)).alias("count"))
            .orderBy(F.desc("count"))
            .limit(k)
            .collect()
        )

        for row in top_k:
            value = row[c]
            cnt = int(row["count"])
            pct = (cnt / total_rows) if total_rows > 0 else None
            summary_rows.append(
                (c, None if value is None else str(value), cnt, float(pct) if pct is not None else None)
            )

    return df.sparkSession.createDataFrame(
        summary_rows,
        schema="column string, value string, count long, pct double",
    )


def profile_numeric_distribution(
    df: DataFrame,
    *,
    max_columns: Optional[int] = None,
) -> DataFrame:
    total_rows = df.count()

    if total_rows == 0 or not df.columns:
        return df.sparkSession.createDataFrame(
            [],
            schema="column string, count long, mean double, stddev double, min double, max double",
        )

    schema_by_name = {field.name: field.dataType for field in df.schema}
    numeric_cols = [
        name for name, dtype in schema_by_name.items() if isinstance(dtype, NumericType)
    ]

    if max_columns is not None:
        numeric_cols = numeric_cols[: max_columns]

    if not numeric_cols:
        return df.sparkSession.createDataFrame(
            [],
            schema="column string, count long, mean double, stddev double, min double, max double",
        )

    summary = (
        df.select([F.col(c).cast("double").alias(c) for c in numeric_cols])
        .summary("count", "mean", "stddev", "min", "max")
        .collect()
    )

    stats_by_col = {c: {} for c in numeric_cols}
    for row in summary:
        stat_name = row["summary"]
        for c in numeric_cols:
            stats_by_col[c][stat_name] = row[c]

    def _to_float(value):
        if value is None:
            return None
        try:
            v = float(value)
        except Exception:
            return None
        if v != v:
            return None
        return v

    summary_rows = []
    for c in numeric_cols:
        stats = stats_by_col.get(c, {})
        count_str = stats.get("count")
        count_val = int(_to_float(count_str)) if count_str is not None else 0
        mean_val = _to_float(stats.get("mean"))
        stddev_val = _to_float(stats.get("stddev"))
        min_val = _to_float(stats.get("min"))
        max_val = _to_float(stats.get("max"))
        summary_rows.append((c, count_val, mean_val, stddev_val, min_val, max_val))

    return df.sparkSession.createDataFrame(
        summary_rows,
        schema="column string, count long, mean double, stddev double, min double, max double",
    )


def print_structural_profile(
    df: DataFrame,
    dataset_name: str,
    *,
    max_missingness_columns: Optional[int] = None,
    top_values_k: int = 5,
    output_base_dir: Optional[str] = None,
) -> None:
    """Print a basic structural + missingness profile for a dataset.

    This is a convenience wrapper intended to be called from ETL scripts so
    that a single function call provides:

    - row/column counts
    - per-column missingness summary
    - suggested standardized (snake_case) column names
    """

    report_sections: List[str] = []

    shape_info = profile_shape(df)
    structural_block = f"{dataset_name} structural profile: {shape_info}"
    print(structural_block)
    report_sections.append(structural_block)

    missingness = profile_missingness(df, max_columns=max_missingness_columns)
    missingness_header = f"{dataset_name} missingness profile:"
    missingness_table = _format_spark_table(missingness)
    print(missingness_header)
    print(missingness_table)
    report_sections.append(missingness_header + "\n" + missingness_table)

    cardinality = profile_cardinality(df)
    cardinality_header = f"{dataset_name} cardinality profile:"
    cardinality_table = _format_spark_table(cardinality)
    print(cardinality_header)
    print(cardinality_table)
    report_sections.append(cardinality_header + "\n" + cardinality_table)

    top_values = profile_top_values(df, k=top_values_k)
    top_values_header = f"{dataset_name} top values (k={top_values_k}):"
    top_values_table = _format_spark_table(top_values)
    print(top_values_header)
    print(top_values_table)
    report_sections.append(top_values_header + "\n" + top_values_table)

    numeric_profile = profile_numeric_distribution(df)
    numeric_header = f"{dataset_name} numeric distribution profile:"
    numeric_table = _format_spark_table(numeric_profile)
    print(numeric_header)
    print(numeric_table)
    report_sections.append(numeric_header + "\n" + numeric_table)

    suggested_cols = standardize_column_names(df.columns)
    suggestions_header = f"{dataset_name} suggested standardized column names:"
    print(suggestions_header)
    suggestion_lines = [suggestions_header]
    for original, cleaned in zip(df.columns, suggested_cols):
        if original != cleaned:
            line = f"  {original} -> {cleaned}"
            print(line)
            suggestion_lines.append(line)
    report_sections.append("\n" + "\n".join(suggestion_lines))

    if output_base_dir is not None:
        dataset_id = re.sub(r"[^0-9a-zA-Z_]+", "_", dataset_name.strip()).lower()
        if not dataset_id:
            dataset_id = "dataset"
        report_dir = os.path.join(output_base_dir, dataset_id, "reports")
        os.makedirs(report_dir, exist_ok=True)
        report_path = os.path.join(report_dir, "preprocessing-assessment-report.txt")
        report_text = "\n\n".join(report_sections) + "\n"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_text)
