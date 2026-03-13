from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, LongType
)

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
