from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)

discussion_forums_schema = StructType([
    StructField("org_unit_id", IntegerType(), False),
    StructField("forum_id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("must_post_to_participate", BooleanType(), True),
    StructField("allow_anon", BooleanType(), True),
    StructField("is_hidden", BooleanType(), True),
    StructField("requires_approval", BooleanType(), True),
    StructField("sort_order", IntegerType(), True),
    StructField("is_deleted", BooleanType(), True),
    StructField("deleted_date", TimestampType(), True),
    StructField("deleted_by_user_id", IntegerType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("start_date", TimestampType(), True),
    StructField("start_date_availability_type", IntegerType(), True),
    StructField("end_date", TimestampType(), True),
    StructField("end_date_availability_type", IntegerType(), True),
    StructField("has_description", BooleanType(), True),
])
