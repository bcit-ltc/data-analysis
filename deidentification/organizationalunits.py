import sys

from pyspark.sql import SparkSession
from schemas.organizationalunits_schemas import org_units_schema
from common.pii_detection import has_email_pattern, has_student_id_pattern, redact_pii_fields
from common.create_pii_report import create_pii_report
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)

DATASET_NAME = "organizationalunits"
DATASET_TABLE = "organizationalunits"

# PII risks: 
# - Condition: None specifically identified from schema alone; table primarily describes courses/org units.
#   Fields: Name, Code, Organization
#   Description: Organizational labels; generally not PII, but could reference individuals if org-unit naming conventions include person names.

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
        .appName("organizationalunits Deidentification")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Load your dataset here
    org_units = read_csv(f"{INPUT_BASE}/{DATASET_NAME}/{DATASET_TABLE}/data", org_units_schema)
    
    total_records = org_units.count()
    
    dropped_columns = ["name", "code"] 
    org_units = org_units.drop(*dropped_columns)
    
    # Redact PII fields instead of dropping rows
    org_units, redaction_stats = redact_pii_fields(
        org_units,
        {
            # "name": "[PII_REDACTED_NAME]",
            "type": "[PII_REDACTED_TYPE]",
            # "code": "[PII_REDACTED_CODE]",
            "organization": "[PII_REDACTED_ORGANIZATION]"
        },
        detection_func=lambda col_name: has_email_pattern(col_name) | has_student_id_pattern(col_name)
    )

    # --- publish dataset ---
    write_csv_publish(org_units, DATASET_NAME, DATASET_TABLE, single_file=True)
    
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
        print("Usage: organizationalunits.py <input_base> <output_base>")
        sys.exit(1)

    input_base = sys.argv[1]
    output_base = sys.argv[2]
    main(input_base, output_base)
