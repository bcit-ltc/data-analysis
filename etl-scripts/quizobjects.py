import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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


DATASET_NAME = "quizzes"
DATASET_TABLE = "quizobjects"

# ---------- schemas ----------
# Quiz Objects: https://community.d2l.com/brightspace/kb/articles/4532-quizzes-data-sets#quiz-objects
quiz_objects_schema = StructType([
    StructField("QuizId", LongType(), False),
    StructField("QuizName", StringType(), True),
    StructField("QuizDescription", StringType(), True),
    StructField("QuizCategory", StringType(), True),
    StructField("IsActive", BooleanType(), True),
    StructField("OrgUnitId", LongType(), False),
    StructField("StartDate", TimestampType(), True),
    StructField("EndDate", TimestampType(), True),
    StructField("DueDate", TimestampType(), True),
    StructField("CreationDate", TimestampType(), True),
    StructField("CreatedBy", LongType(), True),
    StructField("LastModified", TimestampType(), True),
    StructField("LastModifiedBy", LongType(), True),
    StructField("GradeObjectId", LongType(), True),
    StructField("OverallScoreCalculation", StringType(), True),
    StructField("QuizScoreDenominator", DoubleType(), True),
    StructField("HasPassword", BooleanType(), True),
    StructField("IPRestricted", BooleanType(), True),
    StructField("TimeLimit", IntegerType(), True),
    StructField("TimeLimitEnforced", BooleanType(), True),
    StructField("AttemptsAllowed", IntegerType(), True),
    StructField("PreventMovingBackwards", BooleanType(), True),
    StructField("AllowHints", BooleanType(), True),
    StructField("NotificationEmail", StringType(), True),
    StructField("DisablePagerAccess", BooleanType(), True),
    StructField("DisplayInCalendar", BooleanType(), True),
    StructField("IsAttemptRldb", BooleanType(), True),
    StructField("IsSubviewRldb", BooleanType(), True),
    StructField("SortOrder", IntegerType(), True),
    StructField("CategoryId", LongType(), True),
    StructField("ResultId", LongType(), True),
    StructField("IsRetakeIncorrectOnly", BooleanType(), True),
    StructField("PagingTypeId", IntegerType(), True),
    StructField("IsSynchronous", BooleanType(), True),
    StructField("DeductionPercentage", StringType(), True),
    StructField("AIStudySupport", BooleanType(), True),
    StructField("HideQuestionPoints", BooleanType(), True),
])

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
        .appName("Quiz Objects ETL")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Load your dataset here
    quiz_objects_raw = read_csv(f"{RAW_BASE}/QuizObjects/QuizObjects.csv", quiz_objects_schema).cache()

    # --- structural + schema profiling (step 1) ---
    print_structural_profile(
        quiz_objects_raw,
        dataset_name=DATASET_NAME,
        table_name=DATASET_TABLE,
        top_values_k=10,
        output_base_dir=OUT_BASE,
    )

    # --- dataset-specific standardization, validation, and publishing below ---

    # --- standardization + canonicalization (step 2) ---
    df = normalize_column_names(quiz_objects_raw)
    df = canonicalize_nulls(df, columns=[
        "quiz_name",
        "quiz_description",
        "quiz_category",
        "overall_score_calculation",
        "notification_email",
        "deduction_percentage",
    ])
    df = trim_whitespace(df)
    df = standardize_datetimes_iso(df, columns=[
        "start_date",
        "end_date",
        "due_date",
        "creation_date",
        "last_modified",
    ])
    # Flag whether a quiz has a non-null, non-empty description
    df = df.withColumn(
        "has_quiz_description",
        (col("quiz_description").isNotNull()) & (col("quiz_description") != "")
    )
    df = df.cache()



    # --- quality report ---
    print_quality_report(
        df,
        dataset_name=DATASET_NAME,
        key_columns=["quiz_id"],
        numeric_columns=[
            "quiz_id",
            "org_unit_id",
            "created_by",
            "last_modified_by",
            "grade_object_id",
            "quiz_score_denominator",
            "time_limit",
            "attempts_allowed",
            "sort_order",
            "category_id",
            "result_id",
            "paging_type_id",
        ],
        table_name=DATASET_TABLE,
        output_base_dir=OUT_BASE,
    )

    # --- publish cleaned dataset ---
    write_csv_publish(df, DATASET_NAME, DATASET_TABLE)

    quiz_objects_raw.unpersist()
    df.unpersist()


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: quizobjects.py <raw_base> <out_base>")
        sys.exit(1)

    raw_base = sys.argv[1]
    out_base = sys.argv[2]
    main(raw_base, out_base)