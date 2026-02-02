import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)

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

def norm_ws(col):
    # trim + collapse internal whitespace
    return F.trim(F.regexp_replace(col, r"\s+", " "))

def format_ts_for_csv(df, ts_cols):
    # publish timestamps as ISO-like strings (UTC-ish). adjust if you need strict ISO 8601 with 'Z'
    out = df
    for c in ts_cols:
        if c in out.columns:
            out = out.withColumn(c, F.date_format(F.col(c), "yyyy-MM-dd'T'HH:mm:ss"))
    return out

def write_csv_publish(df, name: str, single_file: bool = False):
    out = df
    if single_file:
        out = out.coalesce(1)

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
        .appName("Role Details ETL")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    role_details_raw = read_csv(f"{RAW_BASE}/RoleDetails/RoleDetails.csv", role_details_schema)

    role_details_raw.printSchema()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: roledetails.py <raw_base> <out_base>")
        sys.exit(1)

    raw_base = sys.argv[1]
    out_base = sys.argv[2]
    main(raw_base, out_base)

