from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)

# Organizational Units dataset
# Docs: https://community.d2l.com/brightspace/kb/articles/4529-organizational-units-data-sets
# Schema fields mirror the Brightspace Organizational Units data set columns; see docs for field descriptions.
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
