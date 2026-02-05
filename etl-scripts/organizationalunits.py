import sys
from pyspark.sql import SparkSession
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
        .appName("Org Units ETL")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    org_units_raw = read_csv(f"{RAW_BASE}/OrganizationalUnits/OrganizationalUnits.csv", org_units_schema)
    org_units_raw.show(5)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: organizationalunits.py <raw_base> <out_base>")
        sys.exit(1)

    raw_base = sys.argv[1]
    out_base = sys.argv[2]
    main(raw_base, out_base)

