import sys

from pyspark.sql import SparkSession
from schemas.gradeobjects_schemas import grade_objects_schema
from common.pii_detection import has_email_pattern, has_student_id_pattern, redact_pii_fields
from common.create_pii_report import create_pii_report
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType, DoubleType
)

DATASET_NAME = "grades"
DATASET_TABLE = "gradeobjects"

# PII risks: 
# - Condition: DeletedByUserId IS NOT NULL
#   Fields: DeletedByUserId
#   Description: Numeric identifier of the user who deleted the grade object (audit trail; pseudonymous personal data).
# - Condition: Name or ShortName contains personal names, student IDs, or other identifying text
#   Fields: Name, ShortName
#   Description: Free-text labels for grade items that can include identifiers if authored that way.

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
        .appName("gradeobjects Deidentification")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Load your dataset here
    grade_objects = read_csv(f"{INPUT_BASE}/{DATASET_NAME}/{DATASET_TABLE}/data", grade_objects_schema)
    
    total_records = grade_objects.count()
    
    # Track dropped columns
    dropped_columns = ["deleted_by_user_id"]
    grade_objects = grade_objects.drop(*dropped_columns)

    # - Condition: Name or ShortName contains personal names, student IDs, or other identifying text
    #   Fields: Name, ShortName
    #   Description: Free-text labels for grade items that can include identifiers if authored that way.
    
    # Redact PII fields instead of dropping rows
    grade_objects, redaction_stats = redact_pii_fields(
        grade_objects,
        {
            "name": "[PII_REDACTED_NAME]",
            "short_name": "[PII_REDACTED_SHORT_NAME]"
        },
        detection_func=lambda col_name: has_email_pattern(col_name) | has_student_id_pattern(col_name)
    )

    # --- publish dataset ---
    write_csv_publish(grade_objects, DATASET_NAME, DATASET_TABLE, single_file=True)
    
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
        print("Usage: gradeobjects.py <input_base> <output_base>")
        sys.exit(1)

    input_base = sys.argv[1]
    output_base = sys.argv[2]
    main(input_base, output_base)
