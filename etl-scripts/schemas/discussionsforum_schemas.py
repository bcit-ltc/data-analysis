from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)

# Discussion Forums: https://community.d2l.com/brightspace/kb/articles/4525-discussions-data-sets#discussion-forums
# Types per KB: OrgUnitId int; ForumId bigint; Name nvarchar(400); Description nvarchar(1000);
# MustPostToParticipate, AllowAnon, IsHidden, RequiresApproval, IsDeleted bit; SortOrder int;
# DeletedDate, StartDate, EndDate datetime2; DeletedByUserId, ResultId int; StartDateAvailabilityType, EndDateAvailabilityType smallint
discussion_forums_schema = StructType([
    StructField("OrgUnitId", IntegerType(), False),
    StructField("ForumId", LongType(), False),
    StructField("Name", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("MustPostToParticipate", BooleanType(), True),
    StructField("AllowAnon", BooleanType(), True),
    StructField("IsHidden", BooleanType(), True),
    StructField("RequiresApproval", BooleanType(), True),
    StructField("SortOrder", IntegerType(), True),
    StructField("IsDeleted", BooleanType(), True),
    StructField("DeletedDate", TimestampType(), True),
    StructField("DeletedByUserId", IntegerType(), True),
    StructField("ResultId", IntegerType(), True),
    StructField("StartDate", TimestampType(), True),
    StructField("StartDateAvailabilityType", IntegerType(), True),
    StructField("EndDate", TimestampType(), True),
    StructField("EndDateAvailabilityType", IntegerType(), True),
])
