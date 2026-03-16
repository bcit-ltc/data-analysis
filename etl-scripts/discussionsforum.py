import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from common.structural_schema_profiling import print_structural_profile
from common.standardization_canonicalization import (
    normalize_column_names,
    canonicalize_nulls,
    trim_whitespace,
    standardize_datetimes_iso,
)
from common.quality_validation import print_quality_report
from schemas.discussionsforum_schemas import discussion_forums_schema


DATASET_NAME = "discussions"
DATASET_TABLE = "discussionforums"

# ---------- helpers ----------
def read_csv(path: str, schema: StructType):
    """Read CSV with quote/escape/multiLine so quoted fields do not misalign columns."""
    return (spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .schema(schema)
        .load(path)
    )

def write_csv_publish(df, dataset_name: str, table_name: str, single_file: bool = False):
    out = df.coalesce(1) if single_file else df

    (out.write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .option("quoteAll", "true")
        .option("escape", "\"")
        .option("emptyValue", "")
        .option("nullValue", "")
        .save(f"{OUT_BASE}/{dataset_name}/{table_name}/data")
    )

def main(raw_base: str, out_base: str):
    global RAW_BASE, OUT_BASE, spark

    RAW_BASE = raw_base
    OUT_BASE = out_base

    spark = (
        SparkSession.builder
        .appName("Discussion Forums ETL")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Load your dataset here
    discussion_forums_raw = read_csv(
        f"{RAW_BASE}/DiscussionsForum/DiscussionForums.csv",
        discussion_forums_schema,
    )

    # --- structural + schema profiling (step 1) ---
    print_structural_profile(
        discussion_forums_raw,
        dataset_name=DATASET_NAME,
        table_name=DATASET_TABLE,
        top_values_k=10,
        output_base_dir=OUT_BASE,
    )

    # --- dataset-specific standardization, validation, and publishing below ---

    # --- standardization + canonicalization (step 2) ---
    df = normalize_column_names(discussion_forums_raw)
    # Canonicalize only specific string columns; crash fast if mis-specified
    df = canonicalize_nulls(df, columns=[
        "name",
        "description",
    ])
    # Trim whitespace in all string columns
    df = trim_whitespace(df)
    # Standardize specific timestamp columns
    df = standardize_datetimes_iso(df, columns=[
        "deleted_date",
        "start_date",
        "end_date",
    ])
    # Flag whether a discussion forum has a non-null, non-empty description
    df = df.withColumn(
        "has_description",
        (F.col("description").isNotNull()) & (F.col("description") != "")
    )

    # --- quality report ---
    print_quality_report(
        df,
        dataset_name=DATASET_NAME,
        key_columns=["org_unit_id", "forum_id"],
        numeric_columns=[
            "org_unit_id",
            "forum_id",
            "sort_order",
            "deleted_by_user_id",
            "result_id",
            "start_date_availability_type",
            "end_date_availability_type",
        ],
        table_name=DATASET_TABLE,
        output_base_dir=OUT_BASE,
    )








    # --- publish cleaned dataset ---
    write_csv_publish(df, DATASET_NAME, DATASET_TABLE)



if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: discussionsforum.py <raw_base> <out_base>")
        sys.exit(1)

    raw_base = sys.argv[1]
    out_base = sys.argv[2]
    main(raw_base, out_base)
