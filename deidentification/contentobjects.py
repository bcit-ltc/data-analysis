import sys

from pyspark.sql import SparkSession
from schemas.contentobjects_schemas import content_objects_schema
from common.pii_detection import has_email_pattern, has_student_id_pattern, redact_pii_fields
from common.create_pii_report import create_pii_report
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)

DATASET_NAME = "contentdata"
DATASET_TABLE = "contentobjects"

# PII risks: 
# - Condition: CreatedBy IS NOT NULL
#   Fields: CreatedBy
#   Description: Numeric identifier of the user who created the content object (pseudonymous personal data when user lookup exists).
# - Condition: LastModifiedBy IS NOT NULL
#   Fields: LastModifiedBy
#   Description: Numeric identifier of the user who last modified the content object.
# - Condition: DeletedBy IS NOT NULL
#   Fields: DeletedBy
#   Description: Numeric identifier of the user who deleted the content object.
# - Condition: Title or Location contains personal names, student IDs, or other identifying text
#   Fields: Title, Location
#   Description: Free-text metadata that may embed names, IDs, or other identifiers depending on how instructors author content.

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
        .appName("contentobjects Deidentification")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Load your dataset here
    content_objects= read_csv(f"{INPUT_BASE}/{DATASET_NAME}/{DATASET_TABLE}/data", content_objects_schema)
    
    total_records = content_objects.count()
    
    # Track dropped columns
    dropped_columns = ["created_by", "last_modified_by", "deleted_by"]
    content_objects = content_objects.drop(*dropped_columns)

    # Redact PII fields instead of dropping rows
    content_objects, redaction_stats = redact_pii_fields(
        content_objects,
        {
            "title": "[PII_REDACTED_TITLE]",
            "content_object_type": "[PII_REDACTED_CONTENT_OBJECT_TYPE]",
            "completion_type": "[PII_REDACTED_COMPLETION_TYPE]",
            "location": "[PII_REDACTED_LOCATION]"
        },
        detection_func=lambda col_name: has_email_pattern(col_name) | has_student_id_pattern(col_name)
    )

    # --- publish dataset ---
    write_csv_publish(content_objects, DATASET_NAME, DATASET_TABLE, single_file=True)
    
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
        print("Usage: contentobjects.py <input_base> <output_base>")
        sys.exit(1)

    input_base = sys.argv[1]
    output_base = sys.argv[2]
    main(input_base, output_base)
