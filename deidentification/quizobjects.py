import sys

from pyspark.sql import SparkSession
from schemas.quizobjects_schemas import quiz_objects_schema
from common.pii_detection import has_email_pattern, has_student_id_pattern, redact_pii_fields
from common.create_pii_report import create_pii_report
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType, DoubleType
)

DATASET_NAME = "quizzes"
DATASET_TABLE = "quizobjects"

# PII risks: 
# - Condition: NotificationEmail IS NOT NULL
#   Fields: NotificationEmail
#   Description: Direct email address for quiz notifications; clear PII.
# - Condition: CreatedBy IS NOT NULL
#   Fields: CreatedBy
#   Description: Numeric identifier of the user who created the quiz.
# - Condition: LastModifiedBy IS NOT NULL
#   Fields: LastModifiedBy
#   Description: Numeric identifier of the user who last modified the quiz.
# - Condition: QuizName, QuizDescription, QuizCategory, or OverallScoreCalculation contain personal names, student IDs, or other identifying text
#   Fields: QuizName, QuizDescription, QuizCategory, OverallScoreCalculation
#   Description: Free-text metadata/instructions that can embed personal identifiers depending on authoring practices.

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


def main(input_base: str, output_base: str) -> None:
    global INPUT_BASE, OUT_BASE, spark

    INPUT_BASE = input_base
    OUT_BASE = output_base

    spark = (
        SparkSession.builder
        .appName("quizobjects Deidentification")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Load your dataset here
    quizzes = read_csv(f"{INPUT_BASE}/{DATASET_NAME}/{DATASET_TABLE}/data", quiz_objects_schema)
    
    total_records = quizzes.count()

    # Track dropped columns
    dropped_columns = ["notification_email", "created_by", "last_modified_by", "quiz_description", "quiz_name", "quiz_category"]
    quizzes = quizzes.drop(*dropped_columns)

    # Redact PII fields instead of dropping rows
    quizzes, redaction_stats = redact_pii_fields(
        quizzes,
        {
            # "quiz_name": "[PII_REDACTED_QUIZ_NAME]",
            # "quiz_description": "[PII_REDACTED_QUIZ_DESCRIPTION]",
            # "quiz_category": "[PII_REDACTED_QUIZ_CATEGORY]",
            "overall_score_calculation": "[PII_REDACTED_OVERALL_SCORE_CALCULATION]",
            "deduction_percentage": "[PII_REDACTED_DEDUCTION_PERCENTAGE]"
        },
        detection_func=lambda col_name: has_email_pattern(col_name) | has_student_id_pattern(col_name)
    )

    # --- publish dataset ---
    write_csv_publish(quizzes, DATASET_NAME, DATASET_TABLE, single_file=True)
    
    # --- generate PII report ---
    create_pii_report(
        OUT_BASE,
        DATASET_NAME,
        DATASET_TABLE,
        dropped_columns,
        redaction_stats,
        total_records
    )


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: quizobjects.py <input_base> <output_base>")
        sys.exit(1)

    input_base = sys.argv[1]
    output_base = sys.argv[2]
    main(input_base, output_base)
