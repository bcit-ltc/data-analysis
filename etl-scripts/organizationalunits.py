import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)
from common.structural_schema_profiling import print_structural_profile
from common.standardization_canonicalization import (
    normalize_column_names,
    canonicalize_nulls,
    trim_whitespace,
    normalize_booleans,
    normalize_numeric_strings,
    standardize_datetimes_iso,
)
from common.quality_validation import print_quality_report


DATASET_NAME = "organizationalunits"

# ---------- schemas ----------
org_units_schema = StructType([
    StructField("OrgUnitId", IntegerType(), False),
    StructField("Organization", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Code", StringType(), True),
    StructField("StartDate", TimestampType(), True),
    StructField("EndDate", TimestampType(), True),
    StructField("IsActive", BooleanType(), True),
    StructField("CreatedDate", TimestampType(), True),
    StructField("IsDeleted", BooleanType(), True),
    StructField("DeletedDate", TimestampType(), True),
    StructField("RecycledDate", TimestampType(), True),
    StructField("Version", LongType(), True),
    StructField("OrgUnitTypeId", IntegerType(), True),
])

# ---------- helpers ----------
def read_csv(path: str, schema: StructType):
    return (spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .load(path)
    )

def write_csv_publish(df, name: str, single_file: bool = False):
    out = df.coalesce(1) if single_file else df

    (out.write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .option("quoteAll", "true")
        .option("escape", "\"")
        .option("emptyValue", "")
        .option("nullValue", "")
        .save(f"{OUT_BASE}/{name}")
    )

def main(raw_base: str, out_base: str):
    global RAW_BASE, OUT_BASE, spark

    RAW_BASE = raw_base
    OUT_BASE = out_base

    spark = (
        SparkSession.builder
        .appName("Org Units ETL")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    org_units_raw = read_csv(f"{RAW_BASE}/OrganizationalUnits/OrganizationalUnits.csv", org_units_schema)

    # --- structural + schema profiling (step 1) ---
    print_structural_profile(
        org_units_raw,
        dataset_name=DATASET_NAME,
        top_values_k=10,
        output_base_dir=OUT_BASE,
    )
    

    # --- standardization + canonicalization (step 2) ---
    df = normalize_column_names(org_units_raw)
    # Canonicalize only specific string columns; crash fast if mis-specified
    df = canonicalize_nulls(df, columns=["name", "code"])
    # Trim whitespace in all string columns
    df = trim_whitespace(df)
    # Standardize specific timestamp columns
    df = standardize_datetimes_iso(df, columns=["start_date", "end_date"])

    # --- quality validation + scorecard (step 3) ---
    print_quality_report(
        df,
        dataset_name=DATASET_NAME,
        key_columns=["org_unit_id"],
        numeric_columns=["org_unit_id", "version", "org_unit_type_id"],
        output_base_dir=OUT_BASE,
    )

    # --- publish cleaned dataset ---
    write_csv_publish(df, DATASET_NAME)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: organizationalunits.py <raw_base> <out_base>")
        sys.exit(1)

    raw_base = sys.argv[1]
    out_base = sys.argv[2]
    main(raw_base, out_base)

