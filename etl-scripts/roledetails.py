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

def nullify_blanks(col):
    return F.when(F.trim(F.col(col)) == "", None).otherwise(F.col(col))

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

    # role_details_raw.printSchema()

    from pyspark.sql import functions as F

    # Trim and normalize text fields
    df = role_details_raw.withColumn("RoleName", norm_ws("RoleName")) \
                     .withColumn("RoleAlias", norm_ws("RoleAlias")) \
                     .withColumn("RoleCode", norm_ws("RoleCode")) \
                     .withColumn("Description", norm_ws("Description")) \
                     .withColumn("ClassListRoleName", norm_ws("ClassListRoleName"))

    # Nullify blank text fields
    df = df.withColumn("Description", nullify_blanks("Description"))

    # deduplicate roles 
    df = df.dropDuplicates(["OrgUnitId", "RoleId"])
    
    # add IsDeleted and IsActive columns
    df = df.withColumn("IsDeleted", F.col("DeletedBy").isNotNull()) \
           .withColumn("IsActive", F.col("DeletedBy").isNull())

    roles_active = df.filter(F.col("IsActive"))

    w = Window.partitionBy("OrgUnitId", "RoleId").orderBy(F.col("LastModifiedDate").desc_nulls_last())
    latest = df.withColumn("rn", F.row_number().over(w)) \
            .filter(F.col("rn") == 1) \
            .drop("rn")

    # Feature engineering / derived flags

    df = df.withColumn(
    "IsVisibleToLearners",
    F.col("ShowInContent") |
    F.col("ShowInGrades") |
    F.col("ShowInDiscussionAssess") |
    F.col("ShowInDiscussionStats") |
    F.col("ShowInAttendance") |
    F.col("ShowInUserProgress")
    )

    # Group roles into coarse categories by name/code

    df = df.withColumn(
    "RoleCategory",
    F.when(F.lower("RoleName").like("%student%"), "Student")
     .when(F.lower("RoleName").like("%instructor%"), "Instructor")
     .when(F.lower("RoleName").like("%ta%"), "TA")
     .when(F.lower("RoleName").like("%observer%"), "Observer")
     .otherwise("Other")
    )

    # Combine course access flags

    df = df.withColumn(
    "CanAccessNonCurrentCourses",
    F.col("AccessPastCourses") | F.col("AccessFutureCourses")
    )

    # Modeling for downstream analytics

    summary = df.groupBy("OrgUnitId", "RoleCategory") \
            .agg(F.countDistinct("RoleId").alias("RoleCount"))

    # Quality checks

    bad_rows = df.filter(F.col("RoleId").isNull() | F.col("RoleName").isNull())
    if bad_rows.count() > 0:
        print("Found bad rows:")
        bad_rows.show()
        sys.exit(1)



    # df.printSchema()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: roledetails.py <raw_base> <out_base>")
        sys.exit(1)

    raw_base = sys.argv[1]
    out_base = sys.argv[2]
    main(raw_base, out_base)

