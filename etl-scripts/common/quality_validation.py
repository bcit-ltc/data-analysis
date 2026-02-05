"""Quality validation and scorecard utilities for Spark DataFrames.

This module implements reusable helpers to generate a small quality report so
consumers know what they are getting. The report focuses on:

- missingness percentages
- parse "success" percentages (approximated as non-null ratios)
- duplicate key statistics
- numeric outlier flags

All functions are pure with respect to Spark DataFrames: they do not mutate
inputs in place and instead return new DataFrames or write separate report
artifacts.
"""

import os
from typing import Iterable, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, NumericType, TimestampType

from common.structural_schema_profiling import (
    profile_missingness,
    profile_numeric_distribution,
)


def _format_spark_table(df: DataFrame) -> str:
    """Render a small Spark DataFrame as a plain-text table.

    This is used to embed tabular summaries into text reports.
    """
    cols = df.columns
    if not cols:
        return "(no columns)"

    rows = [row.asDict(recursive=True) for row in df.collect()]
    if not rows:
        return " | ".join(cols)

    col_widths: List[int] = []
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


def _validate_columns_exist(df: DataFrame, columns: Sequence[str]) -> List[str]:
    schema_names = {field.name for field in df.schema}
    requested = list(columns)
    missing = [c for c in requested if c not in schema_names]
    if missing:
        raise ValueError(f"Columns not found in DataFrame: {', '.join(sorted(missing))}")
    return requested


def _duplicate_stats_table(df: DataFrame, key_columns: Sequence[str]) -> DataFrame:
    """Compute duplicate-key statistics as a tiny table.

    The returned DataFrame has schema::

        metric string, value string
    """
    key_columns = _validate_columns_exist(df, key_columns)

    total_rows = df.count()
    key_df = df.select([F.col(c) for c in key_columns])

    distinct_keys = key_df.dropDuplicates().count()
    duplicate_rows = max(total_rows - distinct_keys, 0)
    duplicate_row_pct = (duplicate_rows / total_rows) if total_rows > 0 else None

    keys_with_duplicates = (
        key_df.groupBy(*[F.col(c) for c in key_columns])
        .agg(F.count(F.lit(1)).alias("count"))
        .where(F.col("count") > 1)
        .count()
    )

    rows = [
        ("total_rows", str(total_rows)),
        ("distinct_keys", str(distinct_keys)),
        ("duplicate_rows", str(duplicate_rows)),
        (
            "duplicate_row_pct",
            "" if duplicate_row_pct is None else str(duplicate_row_pct),
        ),
        ("keys_with_duplicates", str(keys_with_duplicates)),
    ]

    return df.sparkSession.createDataFrame(rows, schema="metric string, value string")


def _parse_success_table(
    df: DataFrame,
    missingness_df: DataFrame,
    parsed_columns: Optional[Sequence[str]] = None,
) -> DataFrame:
    """Approximate parse success percentages for selected columns.

    For now this is approximated as the non-null ratio for the given columns,
    based on the missingness profile. For datasets where parsing happens at
    read time (e.g., typed CSV schemas), this still provides a useful signal
    about how much data is actually present/usable.
    """
    total_rows = df.count()
    if total_rows == 0:
        return df.sparkSession.createDataFrame([], schema="column string, parse_success_pct double")

    # Map column -> pct_missing from the missingness profile.
    miss_by_col = {}
    for row in missingness_df.collect():
        miss_by_col[row["column"]] = row["pct_missing"]

    schema_by_name = {field.name: field.dataType for field in df.schema}

    if parsed_columns is None:
        # Default to numeric + temporal columns as the most common parse targets.
        parsed_cols = [
            name
            for name, dtype in schema_by_name.items()
            if isinstance(dtype, (NumericType, DateType, TimestampType))
        ]
    else:
        parsed_cols = _validate_columns_exist(df, parsed_columns)

    rows = []
    for c in parsed_cols:
        pct_missing = miss_by_col.get(c)
        if pct_missing is None:
            # Column not present in missingness table for some reason; skip.
            continue
        success_pct = 1.0 - pct_missing
        rows.append((c, float(success_pct)))

    if not rows:
        return df.sparkSession.createDataFrame([], schema="column string, parse_success_pct double")

    return df.sparkSession.createDataFrame(
        rows,
        schema="column string, parse_success_pct double",
    )


def _numeric_outlier_table(
    df: DataFrame,
    *,
    numeric_columns: Optional[Sequence[str]] = None,
    z_threshold: float = 3.0,
) -> DataFrame:
    """Summarize numeric outliers using a simple z-score rule.

    For each numeric column, counts values where ``|value - mean| > z_threshold
    * stddev`` and reports their proportion of all rows.
    """
    total_rows = df.count()
    if total_rows == 0:
        return df.sparkSession.createDataFrame(
            [],
            schema="column string, n_outliers long, pct_outliers double, rule string",
        )

    schema_by_name = {field.name: field.dataType for field in df.schema}

    if numeric_columns is None:
        cols = [
            name
            for name, dtype in schema_by_name.items()
            if isinstance(dtype, NumericType)
        ]
    else:
        cols = _validate_columns_exist(df, numeric_columns)
        non_numeric = [c for c in cols if not isinstance(schema_by_name.get(c), NumericType)]
        if non_numeric:
            raise TypeError(f"Columns are not numeric-typed: {', '.join(sorted(non_numeric))}")

    if not cols:
        return df.sparkSession.createDataFrame(
            [],
            schema="column string, n_outliers long, pct_outliers double, rule string",
        )

    dist_df = profile_numeric_distribution(df)
    stats = {}
    for row in dist_df.collect():
        stats[row["column"]] = {"mean": row["mean"], "stddev": row["stddev"]}

    rows = []
    for c in cols:
        col_stats = stats.get(c)
        if not col_stats:
            continue
        mean = col_stats["mean"]
        stddev = col_stats["stddev"]
        if mean is None or stddev is None or stddev == 0:
            continue

        n_outliers = df.where(
            F.col(c).isNotNull()
            & (F.abs(F.col(c) - F.lit(mean)) > F.lit(z_threshold * stddev))
        ).count()
        pct_outliers = (n_outliers / total_rows) if total_rows > 0 else None

        rows.append(
            (
                c,
                int(n_outliers),
                float(pct_outliers) if pct_outliers is not None else None,
                f"|value-mean|>{z_threshold}*std",
            )
        )

    if not rows:
        return df.sparkSession.createDataFrame(
            [],
            schema="column string, n_outliers long, pct_outliers double, rule string",
        )

    return df.sparkSession.createDataFrame(
        rows,
        schema="column string, n_outliers long, pct_outliers double, rule string",
    )


def print_quality_report(
    df: DataFrame,
    dataset_name: str,
    *,
    key_columns: Optional[Sequence[str]] = None,
    parsed_columns: Optional[Sequence[str]] = None,
    numeric_columns: Optional[Sequence[str]] = None,
    output_base_dir: Optional[str] = None,
    report_filename: str = "quality-report.txt",
) -> None:
    """Print and optionally persist a small quality report for a dataset.

    Sections include:

    - missingness profile
    - parse success profile
    - duplicate-key statistics (if ``key_columns`` provided)
    - numeric outlier summary

    If ``output_base_dir`` is given, the report is also written to::

        {output_base_dir}/{dataset_name}/reports/{report_filename}
    """

    sections: List[str] = []

    # Missingness profile
    missingness = profile_missingness(df)
    miss_header = f"{dataset_name} missingness profile:"
    miss_table = _format_spark_table(missingness)
    print(miss_header)
    print(miss_table)
    sections.append(miss_header + "\n" + miss_table)

    # Parse success profile (approximate, based on non-null ratios).
    parse_success = _parse_success_table(df, missingness, parsed_columns=parsed_columns)
    if parse_success.count() > 0:
        parse_header = f"{dataset_name} parse success profile (approximate):"
        parse_table = _format_spark_table(parse_success)
        print(parse_header)
        print(parse_table)
        sections.append(parse_header + "\n" + parse_table)

    # Duplicate-key statistics, if key columns are supplied.
    if key_columns is not None:
        dup_table = _duplicate_stats_table(df, key_columns)
        dup_header = f"{dataset_name} duplicate-key summary (keys={list(key_columns)}):"
        dup_table_str = _format_spark_table(dup_table)
        print(dup_header)
        print(dup_table_str)
        sections.append(dup_header + "\n" + dup_table_str)

    # Numeric outlier summary.
    outliers = _numeric_outlier_table(df, numeric_columns=numeric_columns)
    if outliers.count() > 0:
        out_header = f"{dataset_name} numeric outlier summary:"
        out_table = _format_spark_table(outliers)
        print(out_header)
        print(out_table)
        sections.append(out_header + "\n" + out_table)

    # Persist report to disk if requested.
    if output_base_dir is not None and sections:
        report_dir = os.path.join(output_base_dir, dataset_name, "reports")
        os.makedirs(report_dir, exist_ok=True)
        report_path = os.path.join(report_dir, report_filename)
        report_text = "\n\n".join(sections) + "\n"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_text)
