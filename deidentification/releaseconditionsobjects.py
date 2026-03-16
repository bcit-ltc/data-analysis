import sys

from pyspark.sql import SparkSession
from schemas.releaseconditionsobjects_schemas import release_conditions_objects_schema
from common.pii_detection import has_email_pattern, has_student_id_pattern, redact_pii_fields
from common.create_pii_report import create_pii_report
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, LongType
)

DATASET_NAME = "releaseconditions"
DATASET_TABLE = "releaseconditionsobjects"

# PII risks: 
# - Condition: None specifically identified from schema alone; table defines release-condition logic.
#   Fields: Guid1, Guid2, Name
#   Description: GUIDs and names for release conditions. These are unique technical identifiers and labels; they become privacy-relevant only if they encode or are mapped to user identities elsewhere.

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
        .appName("releaseconditionsobjects Deidentification")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Load your dataset here
    release_conditions = read_csv(f"{INPUT_BASE}/{DATASET_NAME}/{DATASET_TABLE}/data", release_conditions_objects_schema)
    
    total_records = release_conditions.count()

    # Track dropped columns
    dropped_columns = []
    release_conditions = release_conditions.drop(*dropped_columns)

    # Redact PII fields instead of dropping rows
    release_conditions, redaction_stats = redact_pii_fields(
        release_conditions,
        {
            "name": "[PII_REDACTED_NAME]",
            "operator_type_desc": "[PII_REDACTED_OPERATOR_TYPE_DESC]",
            "guid_1": "[PII_REDACTED_GUID_1]",
            "guid_2": "[PII_REDACTED_GUID_2]"
        },
        detection_func=lambda col_name: has_email_pattern(col_name) | has_student_id_pattern(col_name)
    )

    # --- publish dataset ---
    write_csv_publish(release_conditions, DATASET_NAME, DATASET_TABLE, single_file=True)

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
        print("Usage: releaseconditionsobjects.py <input_base> <output_base>")
        sys.exit(1)

    input_base = sys.argv[1]
    output_base = sys.argv[2]
    main(input_base, output_base)
