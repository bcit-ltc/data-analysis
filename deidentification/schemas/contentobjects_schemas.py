from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)

content_objects_schema = StructType([
    StructField("content_object_id", IntegerType(), False),
    StructField("org_unit_id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("content_object_type", StringType(), False),
    StructField("completion_type", StringType(), False),
    StructField("parent_content_object_id", IntegerType(), False),
    StructField("location", StringType(), True),
    StructField("start_date", TimestampType(), True),
    StructField("end_date", TimestampType(), True),
    StructField("due_date", TimestampType(), True),
    StructField("object_id_1", IntegerType(), True),
    StructField("object_id_2", IntegerType(), True),
    StructField("object_id_3", IntegerType(), True),
    StructField("last_modified", TimestampType(), False),
    StructField("is_deleted", BooleanType(), False),
    StructField("sort_order", IntegerType(), False),
    StructField("depth", IntegerType(), False),
    StructField("tool_id", IntegerType(), True),
    StructField("is_hidden", BooleanType(), False),
    StructField("result_id", IntegerType(), True),
    StructField("deleted_date", TimestampType(), True),
    StructField("created_by", IntegerType(), True),
    StructField("last_modified_by", IntegerType(), True),
    StructField("deleted_by", IntegerType(), True),
    StructField("ai_utilization", IntegerType(), False),
])