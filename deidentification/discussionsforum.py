import sys

from pyspark.sql import SparkSession
from schemas.discussionsforum_schemas import discussion_forums_schema
from common.pii_detection import has_email_pattern, has_student_id_pattern, redact_pii_fields
from common.create_pii_report import create_pii_report
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)

DATASET_NAME = "discussions"
DATASET_TABLE = "discussionforums"

# PII risks: 
# - Condition: DeletedByUserId IS NOT NULL
#   Fields: DeletedByUserId
#   Description: Numeric identifier of the user who deleted the discussion forum (audit trail; pseudonymous personal data).
# - Condition: Name or Description contains personal names, student IDs, or other identifying text
#   Fields: Name, Description
#   Description: Forum titles and descriptions that may reference individuals if authored that way.

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
        .appName("discussionforums Deidentification")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Load your dataset here
    discussion_forums = read_csv(f"{INPUT_BASE}/{DATASET_NAME}/{DATASET_TABLE}/data", discussion_forums_schema)
    
    total_records = discussion_forums.count()
    
    # Track dropped columns
    dropped_columns = ["deleted_by_user_id"]
    discussion_forums = discussion_forums.drop(*dropped_columns)

    # - Condition: Name or Description contains personal names, student IDs, or other identifying text
    #   Fields: Name, Description
    #   Description: Forum titles and descriptions that may reference individuals if authored that way.
    
    # Filter for PII in name or description fields
    # Email-like text: something@something.tld (case-insensitive)
    # Student-ID–style numbers: any standalone 7–9 digit number
    # pii_discussion_forums = discussion_forums.filter(
    #     has_email_pattern("name", "description")
    #     # | has_student_id_pattern("name", "description")
    # )

    # pii_discussion_forums.show(20, truncate=False)
    
    # Redact PII fields instead of dropping rows
    discussion_forums, redaction_stats = redact_pii_fields(
        discussion_forums,
        {
            "name": "[PII_REDACTED_NAME]",
            "description": "[PII_REDACTED_DESCRIPTION]"
        },
        detection_func=lambda col_name: has_email_pattern(col_name) | has_student_id_pattern(col_name)
    )

    # --- publish dataset ---
    write_csv_publish(discussion_forums, DATASET_NAME, DATASET_TABLE, single_file=True)
    
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
        print("Usage: discussionsforum.py <input_base> <output_base>")
        sys.exit(1)

    input_base = sys.argv[1]
    output_base = sys.argv[2]
    main(input_base, output_base)
