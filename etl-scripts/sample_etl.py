import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F

from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)

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

parents_schema = StructType([
    StructField("OrgUnitId", IntegerType(), False),
    StructField("ParentOrgUnitId", IntegerType(), False),
    StructField("RowVersion", LongType(), True),
    StructField("DateDeleted", TimestampType(), True),
])

ancestors_schema = StructType([
    StructField("OrgUnitId", IntegerType(), False),
    StructField("AncestorOrgUnitId", IntegerType(), False),
])

desc_schema = StructType([
    StructField("OrgUnitId", IntegerType(), False),
    StructField("DescendantOrgUnitId", IntegerType(), False),
])

recent_access_schema = StructType([
    StructField("OrgUnitId", IntegerType(), False),
    StructField("UserId", IntegerType(), False),  # do NOT publish raw
    StructField("LastAccessedDate", TimestampType(), True),
])


def main(path_in: str, path_out: str):
    # Read the CSV file
    df = spark.read.option("header", True) \
        .option("dateFormat", "yyyyMMdd") \
        .schema(CALENDAR_SCHEMA) \
        .csv(path_in)

    days = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]

    # normalize service_id to remove leading/trailing whitespace
    # convert day columns from string "0"/"1" to integer 0/1
    df = df.select(
        f.trim(f.col("service_id")).alias("service_id"),
        *[
            f.when(
                f.trim(f.col(d)) == f.lit("1"),
                f.lit(1),
            ).otherwise(f.lit(0)).alias(d)
            for d in days
        ],
        f.col("start_date"),
        f.col("end_date"),
    )

    # count the number of active days for each service
    # add extra column for active days count
    df = df.withColumn(
        "active_days_count",
        f.col("monday")
        + f.col("tuesday")
        + f.col("wednesday")
        + f.col("thursday")
        + f.col("friday")
        + f.col("saturday")
        + f.col("sunday"),
    )

    # drop services with no active days
    df = df.filter(f.col("active_days_count") > 0)

    # add date sanity checks: drop rows with NULL start/end dates and where start_date > end_date
    df = df.filter(
        (f.col("start_date").isNotNull()) &
        (f.col("end_date").isNotNull()) &
        (f.col("start_date") <= f.col("end_date"))
    )

    # make extra column for active days array, weekday and weekend flags
    df = df.withColumn(
        "active_days_array",
        f.array(
            f.when(f.col("monday") == 1, f.lit("monday")),
            f.when(f.col("tuesday") == 1, f.lit("tuesday")),
            f.when(f.col("wednesday") == 1, f.lit("wednesday")),
            f.when(f.col("thursday") == 1, f.lit("thursday")),
            f.when(f.col("friday") == 1, f.lit("friday")),
            f.when(f.col("saturday") == 1, f.lit("saturday")),
            f.when(f.col("sunday") == 1, f.lit("sunday")),
        )
    ).withColumn(
        "is_weekday",
        (
                f.col("monday")
                + f.col("tuesday")
                + f.col("wednesday")
                + f.col("thursday")
                + f.col("friday")
        )
        == f.lit(5),
    ).withColumn(
        "is_weekend",
        (f.col("saturday") + f.col("sunday")) >= f.lit(1),
    )

    # df.show()

    # Write to parquet
    df.write.mode("overwrite").parquet(path_out)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: calendar_etl.py <release_date> <base_directory>")
        sys.exit(1)

    release = sys.argv[1]
    folder = sys.argv[2]
    path_in = f"{folder}/raw/static/{release}/calendar.txt"
    path_out = f"{folder}/trusted/static/calendar/{release}/"
    spark = SparkSession.builder.appName('Calendar ETL').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(path_in, path_out)
