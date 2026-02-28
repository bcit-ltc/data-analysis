from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, LongType
)

release_conditions_objects_schema = StructType([
    StructField("pre_requisite_id", LongType(), False),
    StructField("result_id", LongType(), False),
    StructField("org_unit_id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("is_negative_condition", BooleanType(), True),
    StructField("pre_requisite_tool_id", IntegerType(), True),
    StructField("id_1", LongType(), True),
    StructField("id_2", LongType(), True),
    StructField("result_tool_id", IntegerType(), True),
    StructField("uses_percentage", BooleanType(), True),
    StructField("operator_type_desc", StringType(), True),
    StructField("version", LongType(), True),
    StructField("guid_1", StringType(), True),
    StructField("guid_2", StringType(), True),
])
