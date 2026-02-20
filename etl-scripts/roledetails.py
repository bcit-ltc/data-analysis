import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, LongType, TimestampType
)
from common.structural_schema_profiling import print_structural_profile
from common.standardization_canonicalization import (
    normalize_column_names,
    canonicalize_nulls,
    trim_whitespace,
)
from common.quality_validation import print_quality_report

DATASET_NAME = "roledetails"
DATASET_TABLE = "roledetails"

# ---------- schemas ----------
role_details_schema = StructType([
    StructField("OrgUnitId", IntegerType(), False),
    StructField("RoleId", IntegerType(), False),
    StructField("RoleName", StringType(), False),
    StructField("Description", StringType(), True),
    StructField("IsCascading", BooleanType(), False),
    StructField("InClassList", BooleanType(), False),
    StructField("ClassListRoleName", StringType(), True),
    StructField("ClassListShowGroups", BooleanType(), False),
    StructField("ClassListShowSections", BooleanType(), False),
    StructField("ClassListDisplayRole", BooleanType(), False),
    StructField("AccessInactiveCO", BooleanType(), False),
    StructField("HasSpecialAccess", BooleanType(), False),
    StructField("AddToCourseOfferingGroups", BooleanType(), False),
    StructField("CanBeAutoEnrolledIntoGroups", BooleanType(), False),
    StructField("AddToCourseOfferingSections", BooleanType(), False),
    StructField("CanBeAutoEnrolledIntoSections", BooleanType(), False),
    StructField("AccessPastCourses", BooleanType(), False),
    StructField("AccessFutureCourses", BooleanType(), False),
    StructField("SortOrder", IntegerType(), False),
    StructField("ShowInContent", BooleanType(), False),
    StructField("ShowInDiscussionAssess", BooleanType(), False),
    StructField("ShowInDiscussionStats", BooleanType(), False),
    StructField("ShowInGrades", BooleanType(), False),
    StructField("ShowInAttendance", BooleanType(), False),
    StructField("AllowSelfEnrollInGroups", BooleanType(), False),
    StructField("ShowInRegistration", BooleanType(), False),
    StructField("ShowInUserProgress", BooleanType(), False),
    StructField("RoleAlias", StringType(), True),
    StructField("RoleCode", StringType(), True),
    StructField("LastModifiedDate", TimestampType(), True),
    StructField("DeletedBy", IntegerType(), True),
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
        .appName("Role Details ETL")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Load your dataset here
    role_details_raw = read_csv(
        f"{RAW_BASE}/RoleDetails/RoleDetails.csv", 
        role_details_schema
    )

    # --- structural + schema profiling (step 1) ---
    print_structural_profile(
        role_details_raw,
        dataset_name=DATASET_NAME,
        table_name=DATASET_TABLE,
        top_values_k=10,
        output_base_dir=OUT_BASE,
    )

    # --- dataset-specific standardization, validation, and publishing below ---

    # --- standardization + canonicalization (step 2) ---
    df = normalize_column_names(role_details_raw)
    # Canonicalize only specific string columns; crash fast if mis-specified
    df = canonicalize_nulls(df, columns=["description", "class_list_role_name", "role_alias", "role_code"])
    # Trim whitespace in all string columns
    df = trim_whitespace(df)

    # --- quality validation + scorecard (step 3) ---
    print_quality_report(
        df,
        dataset_name=DATASET_NAME,
        key_columns=["org_unit_id", "role_id"],
        numeric_columns=[
            "org_unit_id",
            "role_id",
            "sort_order",
            "deleted_by",
        ],
        table_name=DATASET_TABLE,
        output_base_dir=OUT_BASE,
    )


    # --- publish cleaned dataset ---
    write_csv_publish(df, DATASET_NAME, DATASET_TABLE)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: roledetails.py <raw_base> <out_base>")
        sys.exit(1)

    raw_base = sys.argv[1]
    out_base = sys.argv[2]
    main(raw_base, out_base)

