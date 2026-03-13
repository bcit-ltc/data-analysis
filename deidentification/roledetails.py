import sys

from pyspark.sql import SparkSession
from schemas.roledetails_schemas import role_details_schema
from common.pii_detection import has_email_pattern, has_student_id_pattern, redact_pii_fields
from common.create_pii_report import create_pii_report
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType
)

DATASET_NAME = "roledetails"
DATASET_TABLE = "roledetails"

# PII risks: 
# - Condition: DeletedBy IS NOT NULL
#   Fields: DeletedBy
#   Description: Numeric identifier of the user associated with role deletion or modification (audit trail; pseudonymous personal data).
# - Condition: RoleName, Description, ClassListRoleName, RoleAlias, or RoleCode contain personal names, student IDs, or other identifying text
#   Fields: RoleName, Description, ClassListRoleName, RoleAlias, RoleCode
#   Description: Free-text role metadata that could embed identifiers if roles are named after specific individuals.

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
        .appName("roledetails Deidentification")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Load your dataset here
    role_details = read_csv(f"{INPUT_BASE}/{DATASET_NAME}/{DATASET_TABLE}/data", role_details_schema)
    
    total_records = role_details.count()
    
    # Track dropped columns
    dropped_columns = ["deleted_by", "role_name"]
    role_details = role_details.drop(*dropped_columns)

    # Redact PII fields instead of dropping rows
    role_details, redaction_stats = redact_pii_fields(
        role_details,
        {
            # "role_name": "[PII_REDACTED_ROLE_NAME]",
            "description": "[PII_REDACTED_DESCRIPTION]",
            "class_list_role_name": "[PII_REDACTED_CLASS_LIST_ROLE_NAME]",
            "role_alias": "[PII_REDACTED_ROLE_ALIAS]",
            "role_code": "[PII_REDACTED_ROLE_CODE]"
        },
        detection_func=lambda col_name: has_email_pattern(col_name) | has_student_id_pattern(col_name)
    )

    # --- publish dataset ---
    write_csv_publish(role_details, DATASET_NAME, DATASET_TABLE, single_file=True)
    
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
        print("Usage: roledetails.py <input_base> <output_base>")
        sys.exit(1)

    input_base = sys.argv[1]
    output_base = sys.argv[2]
    main(input_base, output_base)
