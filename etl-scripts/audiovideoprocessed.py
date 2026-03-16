import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from common.structural_schema_profiling import print_structural_profile
from common.standardization_canonicalization import (
    normalize_column_names,
    canonicalize_nulls,
    trim_whitespace,
    standardize_datetimes_iso,
)
from common.quality_validation import print_quality_report
from schemas.audiovideoprocessed_schemas import audio_video_processed_schema


DATASET_NAME = "contentservice"
DATASET_TABLE = "audiovideoprocessed"

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
        .appName("Audio Video Processed")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Load your dataset here
    audio_video_processed_raw = read_csv(f"{RAW_BASE}/ContentService/AudioVideoProcessed.csv", audio_video_processed_schema)

    # --- structural + schema profiling (step 1) ---
    print_structural_profile(
        audio_video_processed_raw,
        dataset_name=DATASET_NAME,
        top_values_k=10,
        output_base_dir=OUT_BASE,
    )

    # --- dataset-specific standardization, validation, and publishing below ---

    # --- standardization + canonicalization (step 2) ---
    df = normalize_column_names(audio_video_processed_raw)
    # Canonicalize only specific string columns; crash fast if mis-specified
    df = canonicalize_nulls(df, columns=["content_id", "revision_id"])
    # Trim whitespace in all string columns
    df = trim_whitespace(df)
    # Standardize specific timestamp columns
    df = standardize_datetimes_iso(df, columns=["last_modified"])

    # --- quality report ---
    print_quality_report(
        df,
        dataset_name=DATASET_NAME,
        key_columns=["content_id", "revision_id"],
        numeric_columns=["revision_number", "revision_size", "duration"],
        output_base_dir=OUT_BASE,
    )








    # --- publish cleaned dataset ---
    write_csv_publish(df, DATASET_NAME, DATASET_TABLE)



if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: audiovideoprocessed.py <raw_base> <out_base>")
        sys.exit(1)

    raw_base = sys.argv[1]
    out_base = sys.argv[2]
    main(raw_base, out_base)
