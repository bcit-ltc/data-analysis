import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType
from common.structural_schema_profiling import print_structural_profile
from common.standardization_canonicalization import (
    normalize_column_names,
    canonicalize_nulls,
    trim_whitespace,
    normalize_booleans,
    normalize_numeric_strings,
    standardize_datetimes_iso,
)
from common.quality_validation import print_quality_report, filter_allowed_values
from schemas.contentobjects_schemas import content_objects_schema


DATASET_NAME = "contentdata"
DATASET_TABLE = "contentobjects"

# ---------- helpers ----------
def read_csv(path: str, schema: StructType):
    return (spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("quote", "\"")
        .option("escape", "\"")
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
        .appName("Content Objects ETL")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Load your dataset here
    content_objects_raw = read_csv(f"{RAW_BASE}/Content/ContentObjects.csv", content_objects_schema)

    # Original D2L timestamp columns that can contain garbage or extreme year values.
    # We will null out any values whose year falls outside the safe Python datetime
    # range so later profiling/collection steps cannot raise overflow errors.
    timestamp_columns = [
        "StartDate",
        "EndDate",
        "DueDate",
        "LastModified",
        "DeletedDate",
    ]
    # Only clamp columns that actually exist in the current file/schema.
    # Values with a year < 1 or > 9999 are treated as invalid and set to NULL.
    for col_name in timestamp_columns:
        if col_name in content_objects_raw.columns:
            content_objects_raw = content_objects_raw.withColumn(
                col_name,
                F.when(
                    F.col(col_name).isNotNull()
                    & (
                        (F.year(F.col(col_name)) < F.lit(1))
                        | (F.year(F.col(col_name)) > F.lit(9999))
                    ),
                    F.lit(None).cast("timestamp"),
                ).otherwise(F.col(col_name)),
            )

    # --- structural + schema profiling (step 1) ---
    print_structural_profile(
        content_objects_raw,
        dataset_name=DATASET_NAME,
        table_name=DATASET_TABLE,
        top_values_k=10,
        output_base_dir=OUT_BASE,
    )

    # --- dataset-specific standardization, validation, and publishing below ---

    # --- standardization + canonicalization (step 2) ---
    df = normalize_column_names(content_objects_raw)
    # Canonicalize only specific string columns; crash fast if mis-specified
    df = canonicalize_nulls(df, columns=[
        "title",
        "content_object_type",
        "completion_type",
        "location",
    ])
    # Trim whitespace in all string columns
    df = trim_whitespace(df)
    # Standardize specific timestamp columns
    df = standardize_datetimes_iso(df, columns=[
        "start_date",
        "end_date",
        "due_date",
        "last_modified",
        "deleted_date",
    ])

    # completion type should only have 4 possibilities:
    # CompletionType        | Auto                                                                                    | 11709147 | 0.9915587728096302    
    # CompletionType        | Manual                                                                                  | 8254     | 0.000698968602133929  
    # CompletionType        | Topic                                                                                   | 943      | 7.985551148683002e-05 
    # CompletionType        | Module   
    allowed_completion_types = [
        "Auto",
        "Manual",
        "Topic",
        "Module",
    ]

    df, completion_filter_section = filter_allowed_values(
        df,
        column="completion_type",
        allowed_values=allowed_completion_types,
        label=f"{DATASET_NAME}.{DATASET_TABLE} completion_type allowed-values filter summary",
    )

    # --- quality report ---
    print_quality_report(
        df,
        dataset_name=DATASET_NAME,
        key_columns=["content_object_id"],
        numeric_columns=[
            "sort_order",
            "depth",
            "ai_utilization",
        ],
        table_name=DATASET_TABLE,
        output_base_dir=OUT_BASE,
        extra_sections=[completion_filter_section],
    )


    # --- publish cleaned dataset ---
    write_csv_publish(df, DATASET_NAME, DATASET_TABLE)



if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: contentobjects.py <raw_base> <out_base>")
        sys.exit(1)

    raw_base = sys.argv[1]
    out_base = sys.argv[2]
    main(raw_base, out_base)

