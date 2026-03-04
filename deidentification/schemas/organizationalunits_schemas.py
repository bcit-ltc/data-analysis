from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)

org_units_schema = StructType([
    StructField("org_unit_id", IntegerType(), False),
    StructField("organization", StringType(), True),
    StructField("type", StringType(), True),
    StructField("name", StringType(), True),
    StructField("code", StringType(), True),
    StructField("start_date", TimestampType(), True),
    StructField("end_date", TimestampType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("created_date", TimestampType(), True),
    StructField("is_deleted", BooleanType(), True),
    StructField("deleted_date", TimestampType(), True),
    StructField("recycled_date", TimestampType(), True),
    StructField("version", LongType(), True),
    StructField("org_unit_type_id", IntegerType(), True),
])
