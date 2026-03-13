import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, LongType
)
from common.structural_schema_profiling import print_structural_profile
from common.standardization_canonicalization import (
    normalize_column_names,
    canonicalize_nulls,
    trim_whitespace,
)
from common.quality_validation import print_quality_report


DATASET_NAME = "releaseconditions"
DATASET_TABLE = "releaseconditionsobjects"

# ---------- schemas ----------
release_conditions_objects_schema = StructType([
    StructField("PreRequisiteId", LongType(), False),
    StructField("ResultId", LongType(), False),
    StructField("OrgUnitId", LongType(), False),
    StructField("Name", StringType(), True),
    StructField("IsNegativeCondition", BooleanType(), True),
    StructField("PreRequisiteToolId", IntegerType(), True),
    StructField("Id1", LongType(), True),
    StructField("Id2", LongType(), True),
    StructField("ResultToolId", IntegerType(), True),
    StructField("UsesPercentage", BooleanType(), True),
    StructField("OperatorTypeDesc", StringType(), True),
    StructField("Version", LongType(), True),
    StructField("Guid1", StringType(), True),
    StructField("Guid2", StringType(), True),
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
        .appName("Release Conditions Objects ETL")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Load your dataset here
    release_conditions_objects_raw = read_csv(
        f"{RAW_BASE}/ReleaseConditionsObjects/ReleaseConditionObjects.csv",
        release_conditions_objects_schema,
    )

    # --- structural + schema profiling (step 1) ---
    print_structural_profile(
        release_conditions_objects_raw,
        dataset_name=DATASET_NAME,
        table_name=DATASET_TABLE,
        top_values_k=10,
        output_base_dir=OUT_BASE,
    )

    # --- dataset-specific standardization, validation, and publishing below ---

    # --- standardization + canonicalization (step 2) ---
    df = normalize_column_names(release_conditions_objects_raw)
    # Canonicalize only specific string columns; crash fast if mis-specified
    df = canonicalize_nulls(df, columns=["name", "operator_type_desc", "guid1", "guid2"])
    # Trim whitespace in all string columns
    df = trim_whitespace(df)

    # --- quality report ---
    print_quality_report(
        df,
        dataset_name=DATASET_NAME,
        key_columns=["pre_requisite_id", "result_id", "org_unit_id"],
        numeric_columns=[
            "pre_requisite_id",
            "result_id",
            "org_unit_id",
            "pre_requisite_tool_id",
            "id1",
            "id2",
            "result_tool_id",
            "version",
        ],
        table_name=DATASET_TABLE,
        output_base_dir=OUT_BASE,
    )








    # --- publish cleaned dataset ---
    write_csv_publish(df, DATASET_NAME, DATASET_TABLE)



if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: releaseconditionsobjects.py <raw_base> <out_base>")
        sys.exit(1)

    raw_base = sys.argv[1]
    out_base = sys.argv[2]
    main(raw_base, out_base)
