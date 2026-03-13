import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)
from common.structural_schema_profiling import print_structural_profile
from common.standardization_canonicalization import (
    normalize_column_names,
    canonicalize_nulls,
    trim_whitespace,
    normalize_booleans,
    normalize_numeric_strings,
    standardize_datetimes_iso,
)
from common.quality_validation import print_quality_report


DATASET_NAME = "contentdata"
DATASET_TABLE = "contentobjects"

# ---------- schemas ----------
content_objects_schema = StructType([
    StructField("ContentObjectId", IntegerType(), False),
    StructField("OrgUnitId", IntegerType(), False),
    StructField("Title", StringType(), False),
    StructField("ContentObjectType", StringType(), False),
    StructField("CompletionType", StringType(), False),
    StructField("ParentContentObjectId", IntegerType(), False),
    StructField("Location", StringType(), True),
    StructField("StartDate", TimestampType(), True),
    StructField("EndDate", TimestampType(), True),
    StructField("DueDate", TimestampType(), True),
    StructField("ObjectId1", IntegerType(), True),
    StructField("ObjectId2", IntegerType(), True),
    StructField("ObjectId3", IntegerType(), True),
    StructField("LastModified", TimestampType(), False),
    StructField("IsDeleted", BooleanType(), False),
    StructField("SortOrder", IntegerType(), False),
    StructField("Depth", IntegerType(), False),
    StructField("ToolId", IntegerType(), True),
    StructField("IsHidden", BooleanType(), False),
    StructField("ResultId", IntegerType(), True),
    StructField("DeletedDate", TimestampType(), True),
    StructField("CreatedBy", IntegerType(), True),
    StructField("LastModifiedBy", IntegerType(), True),
    StructField("DeletedBy", IntegerType(), True),
    StructField("AIUtilization", IntegerType(), False),
])

# ---------- helpers ----------
def read_csv(path: str, schema: StructType):
    return (spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
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

