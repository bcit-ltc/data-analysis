from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)

# Content Objects dataset (Content)
# Docs: https://community.d2l.com/brightspace/kb/articles/4713-content-data-sets
# Schema fields mirror the Brightspace Content Objects data set columns; see docs for field descriptions.
content_objects_schema = StructType([
    StructField("ContentObjectId", IntegerType(), False),
    StructField("OrgUnitId", IntegerType(), False),
    StructField("Title", StringType(), False),
    StructField("ContentObjectType", StringType(), False),
    StructField("CompletionType", StringType(), False),
    StructField("ParentContentObjectId", IntegerType(), False),
    StructField("Location", StringType(), True),
    StructField("StartDate", TimestampType(), True),
    StructField("EndDate", TimestampType(), True),
    StructField("DueDate", TimestampType(), True),
    StructField("ObjectId1", IntegerType(), True),
    StructField("ObjectId2", IntegerType(), True),
    StructField("ObjectId3", IntegerType(), True),
    StructField("LastModified", TimestampType(), False),
    StructField("IsDeleted", BooleanType(), False),
    StructField("SortOrder", IntegerType(), False),
    StructField("Depth", IntegerType(), False),
    StructField("ToolId", IntegerType(), True),
    StructField("IsHidden", BooleanType(), False),
    StructField("ResultId", IntegerType(), True),
    StructField("DeletedDate", TimestampType(), True),
    StructField("CreatedBy", IntegerType(), True),
    StructField("LastModifiedBy", IntegerType(), True),
    StructField("DeletedBy", IntegerType(), True),
    StructField("AIUtilization", IntegerType(), False),
])
