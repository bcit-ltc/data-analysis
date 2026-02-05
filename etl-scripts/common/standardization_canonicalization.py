"""Standardization and canonicalization utilities for Spark DataFrames.

This module implements reusable helpers for step 2 of the data readiness
pipeline:

- canonicalize null-like representations
- trim and normalize whitespace
- normalize boolean-like values
- normalize numeric strings (commas/currency -> numeric)
- standardize date/time values to ISO-8601 strings

All functions are pure: they return a new ``DataFrame`` and do not mutate
inputs in place.
"""

from typing import Iterable, List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, NumericType, DateType, TimestampType
from common.structural_schema_profiling import standardize_column_names


# Default set of string sentinel values that will be treated as "null-like"
# when canonicalising string columns.
_DEFAULT_NULL_SENTINELS = (
    "n/a",
    "na",
    "n.a.",
    "none",
    "null",
    "unknown",
    "unspecified",
    "missing",
)


def _string_columns(df: DataFrame, columns: Optional[Iterable[str]] = None) -> List[str]:
    schema_by_name = {field.name: field.dataType for field in df.schema}

    if columns is None:
        return [
            name for name, dtype in schema_by_name.items()
            if isinstance(dtype, StringType)
        ]

    requested = list(columns)
    missing = [c for c in requested if c not in schema_by_name]
    if missing:
        raise ValueError(f"Columns not found in DataFrame: {', '.join(sorted(missing))}")

    non_string = [
        c for c in requested
        if not isinstance(schema_by_name.get(c), StringType)
    ]
    if non_string:
        raise TypeError(f"Columns are not string-typed: {', '.join(sorted(non_string))}")

    return requested


def canonicalize_nulls(
    df: DataFrame,
    columns: Optional[Iterable[str]] = None,
    *,
    extra_sentinels: Optional[Sequence[str]] = None,
) -> DataFrame:
    """Canonicalize null-like string values to actual nulls.

    For the selected string columns, converts empty/whitespace-only strings and
    common textual sentinels (e.g., "N/A", "unknown") to ``NULL``.
    """
    string_cols = _string_columns(df, columns)
    if not string_cols:
        return df

    schema_by_name = {field.name: field.dataType for field in df.schema}

    sentinels = {s.lower() for s in _DEFAULT_NULL_SENTINELS}
    if extra_sentinels:
        sentinels.update(s.lower() for s in extra_sentinels)

    out = df
    for c in string_cols:
        col_expr = F.col(c)
        lowered_trimmed = F.lower(F.trim(col_expr))
        cond = (lowered_trimmed == "") | lowered_trimmed.isin(*sorted(sentinels))
        target_type = schema_by_name[c]
        out = out.withColumn(
            c,
            F.when(cond, F.lit(None).cast(target_type)).otherwise(col_expr),
        )

    return out


def trim_whitespace(
    df: DataFrame,
    columns: Optional[Iterable[str]] = None,
    *,
    collapse_internal: bool = True,
) -> DataFrame:
    """Trim (and optionally normalize) whitespace in string columns.

    Parameters
    ----------
    collapse_internal:
        If ``True``, collapse internal runs of whitespace to a single space in
        addition to trimming leading/trailing whitespace.
    """
    string_cols = _string_columns(df, columns)
    if not string_cols:
        return df

    out = df
    for c in string_cols:
        col_expr = F.col(c)
        if collapse_internal:
            col_expr = F.regexp_replace(col_expr, r"\s+", " ")
        out = out.withColumn(c, F.trim(col_expr))

    return out


# Canonical boolean tokens recognised in string columns.
_DEFAULT_TRUE_TOKENS = ("y", "yes", "true", "t", "1")
_DEFAULT_FALSE_TOKENS = ("n", "no", "false", "f", "0")


def normalize_booleans(
    df: DataFrame,
    columns: Optional[Iterable[str]] = None,
    *,
    true_values: Optional[Sequence[str]] = None,
    false_values: Optional[Sequence[str]] = None,
    output_type: str = "boolean",
) -> DataFrame:
    """Normalize boolean-like string columns to a canonical form.

    Parameters
    ----------
    columns:
        Optional subset of columns to process. If omitted, all string columns
        are considered.
    true_values / false_values:
        Extra tokens (in addition to defaults like "y", "yes", "1") that
        should be treated as ``True`` / ``False``. Comparisons are case- and
        whitespace-insensitive.
    output_type:
        Either ``"boolean"`` (default) to cast to a ``BooleanType`` column or
        ``"string"`` to keep the column as strings with values "true"/"false".
    """
    string_cols = _string_columns(df, columns)
    if not string_cols:
        return df

    true_tokens = {t.lower() for t in _DEFAULT_TRUE_TOKENS}
    false_tokens = {t.lower() for t in _DEFAULT_FALSE_TOKENS}
    if true_values:
        true_tokens.update(t.lower() for t in true_values)
    if false_values:
        false_tokens.update(t.lower() for t in false_values)

    out = df
    for c in string_cols:
        col_expr = F.col(c)
        lowered_trimmed = F.lower(F.trim(col_expr))
        is_true = lowered_trimmed.isin(*sorted(true_tokens))
        is_false = lowered_trimmed.isin(*sorted(false_tokens))

        if output_type == "boolean":
            new_col = (
                F.when(is_true, F.lit(True))
                .when(is_false, F.lit(False))
                .otherwise(F.lit(None).cast("boolean"))
            )
        elif output_type == "string":
            new_col = (
                F.when(is_true, F.lit("true"))
                .when(is_false, F.lit("false"))
                .otherwise(col_expr)
            )
        else:
            raise ValueError(f"Unsupported output_type for normalize_booleans: {output_type}")

        out = out.withColumn(c, new_col)

    return out


def normalize_numeric_strings(
    df: DataFrame,
    columns: Optional[Iterable[str]] = None,
    *,
    output_type: str = "double",
) -> DataFrame:
    """Normalize numeric-like string columns and cast to numeric type.

    This helper targets columns where numbers are stored as strings with commas,
    currency symbols, or other non-digit characters. It removes common
    formatting characters and casts the result to a numeric type.

    Parameters
    ----------
    output_type:
        Spark SQL type string to cast to (e.g., "double", "decimal(18,2)").
    """
    string_cols = _string_columns(df, columns)
    if not string_cols:
        return df

    out = df
    for c in string_cols:
        col_expr = F.col(c)
        # Remove common thousands separators and currency symbols, keep digits,
        # decimal point, and sign.
        cleaned = F.regexp_replace(col_expr, r"[,$]", "")
        cleaned = F.regexp_replace(cleaned, r"[^0-9+\-\.]", "")
        out = out.withColumn(c, cleaned.cast(output_type))

    return out


def standardize_datetimes_iso(
    df: DataFrame,
    columns: Optional[Iterable[str]] = None,
    *,
    output_format: str = "yyyy-MM-dd'T'HH:mm:ss",
) -> DataFrame:
    """Standardize date/time columns to ISO-8601-like strings.

    For columns of ``DateType`` or ``TimestampType``, formats values using the
    provided Spark SQL ``date_format`` pattern (ISO-8601-like by default).
    String-typed columns are left unchanged; callers should cast them to
    ``TimestampType``/``DateType`` before invoking this helper.
    """
    schema_by_name = {field.name: field.dataType for field in df.schema}

    if columns is None:
        target_cols = [
            name for name, dtype in schema_by_name.items()
            if isinstance(dtype, (DateType, TimestampType))
        ]
    else:
        requested = list(columns)
        missing = [c for c in requested if c not in schema_by_name]
        if missing:
            raise ValueError(f"Columns not found in DataFrame: {', '.join(sorted(missing))}")

        non_temporal = [
            c for c in requested
            if not isinstance(schema_by_name.get(c), (DateType, TimestampType))
        ]
        if non_temporal:
            raise TypeError(
                "Columns are not DateType/TimestampType: "
                + ", ".join(sorted(non_temporal))
            )

        target_cols = requested

    if not target_cols:
        return df

    out = df
    for c in target_cols:
        out = out.withColumn(c, F.date_format(F.col(c), output_format))

    return out


def normalize_column_names(
    df: DataFrame,
    *,
    case: str = "snake",
    strip_whitespace: bool = True,
) -> DataFrame:
    """Return a new DataFrame with normalized column names.

    This helper delegates the actual naming logic to
    :func:`common.structural_schema_profiling.standardize_column_names` and
    applies the resulting names to the given ``DataFrame``.

    Parameters
    ----------
    df:
        Input Spark ``DataFrame`` whose columns should be renamed.
    case:
        Naming style to target (e.g., ``"snake"``, ``"lower"``, ``"upper"``).
    strip_whitespace:
        Whether to trim leading/trailing whitespace in the original names
        before normalisation.
    """

    original_cols = list(df.columns)
    new_cols = standardize_column_names(
        original_cols,
        case=case,
        strip_whitespace=strip_whitespace,
    )

    if len(new_cols) != len(original_cols):
        raise RuntimeError(
            "standardize_column_names returned a different number of names "
            "than there are DataFrame columns"
        )

    # Guard against accidental name collisions, which make DataFrame
    # operations ambiguous and hard to reason about.
    seen = set()
    duplicates = []
    for name in new_cols:
        if name in seen:
            duplicates.append(name)
        else:
            seen.add(name)

    if duplicates:
        dup_list = ", ".join(sorted(set(duplicates)))
        raise ValueError(
            "Normalized column names are not unique; "
            f"conflicting names: {dup_list}"
        )

    return df.toDF(*new_cols)
