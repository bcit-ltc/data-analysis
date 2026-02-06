import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType, DoubleType
)
from common.structural_schema_profiling import print_structural_profile
from common.standardization_canonicalization import (
    normalize_column_names,
    canonicalize_nulls,
    trim_whitespace,
    standardize_datetimes_iso,
)
from common.quality_validation import print_quality_report


DATASET_NAME = "grades"
DATASET_TABLE = "gradeobjects"

# ---------- schemas ----------
grade_objects_schema = StructType([
    StructField("GradeObjectId", LongType(), False),
    StructField("OrgUnitId", LongType(), False),
    StructField("ParentGradeObjectId", LongType(), True),
    StructField("Name", StringType(), True),
    StructField("TypeName", StringType(), True),
    StructField("StartDate", TimestampType(), True),
    StructField("EndDate", TimestampType(), True),
    StructField("IsAutoPointed", BooleanType(), True),
    StructField("IsFormula", BooleanType(), True),
    StructField("IsBonus", BooleanType(), True),
    StructField("MaxPoints", DoubleType(), True),
    StructField("CanExceedMaxGrade", BooleanType(), True),
    StructField("ExcludeFromFinalGradeCalc", BooleanType(), True),
    StructField("GradeSchemeId", LongType(), True),
    StructField("Weight", DoubleType(), True),
    StructField("NumLowestGradesToDrop", IntegerType(), True),
    StructField("NumHighestGradesToDrop", IntegerType(), True),
    StructField("WeightDistributionType", StringType(), True),
    StructField("CreatedDate", TimestampType(), True),
    StructField("ToolName", StringType(), True),
    StructField("AssociatedToolItemId", LongType(), True),
    StructField("LastModified", TimestampType(), True),
    StructField("ShortName", StringType(), True),
    StructField("GradeObjectTypeId", IntegerType(), True),
    StructField("SortOrder", IntegerType(), True),
    StructField("IsDeleted", BooleanType(), True),
    StructField("DeletedDate", TimestampType(), True),
    StructField("DeletedByUserId", LongType(), True),
    StructField("ResultId", LongType(), True),
    StructField("ToolId", IntegerType(), True),
    StructField("Version", LongType(), True),
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
        .appName("Grade Objects ETL")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Load your dataset here
    grade_objects_raw = read_csv(
        f"{RAW_BASE}/GradeObjects/GradeObjects.csv",
        grade_objects_schema,
    )

    # --- structural + schema profiling (step 1) ---
    print_structural_profile(
        grade_objects_raw,
        dataset_name=DATASET_NAME,
        table_name=DATASET_TABLE,
        top_values_k=10,
        output_base_dir=OUT_BASE,
    )

    # --- dataset-specific standardization, validation, and publishing below ---

    # --- standardization + canonicalization (step 2) ---
    df = normalize_column_names(grade_objects_raw)
    # Canonicalize only specific string columns; crash fast if mis-specified
    df = canonicalize_nulls(df, columns=[
        "name",
        "type_name",
        "weight_distribution_type",
        "tool_name",
        "short_name",
    ])
    # Trim whitespace in all string columns
    df = trim_whitespace(df)
    # Standardize specific timestamp columns
    df = standardize_datetimes_iso(df, columns=[
        "start_date",
        "end_date",
        "created_date",
        "last_modified",
        "deleted_date",
    ])

    # --- quality validation + scorecard (step 3) ---
    print_quality_report(
        df,
        dataset_name=DATASET_NAME,
        key_columns=["grade_object_id"],
        numeric_columns=[
            "grade_object_id",
            "org_unit_id",
            "parent_grade_object_id",
            "max_points",
            "grade_scheme_id",
            "weight",
            "num_lowest_grades_to_drop",
            "num_highest_grades_to_drop",
            "associated_tool_item_id",
            "grade_object_type_id",
            "sort_order",
            "deleted_by_user_id",
            "result_id",
            "tool_id",
            "version",
        ],
        table_name=DATASET_TABLE,
        output_base_dir=OUT_BASE,
    )








    # --- publish cleaned dataset ---
    write_csv_publish(df, DATASET_NAME, DATASET_TABLE)



if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: gradeobjects.py <raw_base> <out_base>")
        sys.exit(1)

    raw_base = sys.argv[1]
    out_base = sys.argv[2]
    main(raw_base, out_base)
