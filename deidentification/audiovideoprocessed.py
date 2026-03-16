import sys

from pyspark.sql import SparkSession
from schemas.audiovideoprocessed_schemas import audio_video_processed_schema
from common.pii_detection import has_email_pattern, has_student_id_pattern, redact_pii_fields
from common.create_pii_report import create_pii_report
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)

DATASET_NAME = "contentservice"
DATASET_TABLE = "audiovideoprocessed"

# PII risks: 
# - Condition: Any row where this dataset is joined to user-identifying tables using ContentId or RevisionId.
#   Fields: ContentId, RevisionId
#   Description: Technical content identifiers that become personal data when mapped to specific users or enrollments.

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
        .appName("audiovideoprocessed Deidentification")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Load your dataset here
    audio_video = read_csv(f"{INPUT_BASE}/{DATASET_NAME}/{DATASET_TABLE}/data", audio_video_processed_schema)
    
    total_records = audio_video.count()

    # Track dropped columns
    dropped_columns = []
    audio_video = audio_video.drop(*dropped_columns)

    # Redact PII fields instead of dropping rows
    audio_video, redaction_stats = redact_pii_fields(
        audio_video,
        {
            "content_id": "[PII_REDACTED_CONTENT_ID]",
            "revision_id": "[PII_REDACTED_REVISION_ID]",
            "type": "[PII_REDACTED_TYPE]",
            "source": "[PII_REDACTED_SOURCE]"
        },
        detection_func=lambda col_name: has_email_pattern(col_name) | has_student_id_pattern(col_name)
    )

    # --- publish dataset ---
    write_csv_publish(audio_video, DATASET_NAME, DATASET_TABLE, single_file=True)
    
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
        print("Usage: audiovideoprocessed.py <input_base> <output_base>")
        sys.exit(1)

    input_base = sys.argv[1]
    output_base = sys.argv[2]
    main(input_base, output_base)