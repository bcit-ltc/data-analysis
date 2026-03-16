from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType, DoubleType
)

# Grade Objects dataset (Grades)
# Docs: https://community.d2l.com/brightspace/kb/articles/4527-grades-data-sets
# Schema fields mirror the Brightspace Grade Objects data set columns; see docs for field descriptions.
grade_objects_schema = StructType([
    StructField("GradeObjectId", LongType(), False),
    StructField("OrgUnitId", LongType(), False),
    StructField("ParentGradeObjectId", LongType(), True),
    StructField("Name", StringType(), True),
    StructField("TypeName", StringType(), True),
    StructField("StartDate", TimestampType(), True),
    StructField("EndDate", TimestampType(), True),
    StructField("IsAutoPointed", BooleanType(), True),
    StructField("IsFormula", BooleanType(), True),
    StructField("IsBonus", BooleanType(), True),
    StructField("MaxPoints", DoubleType(), True),
    StructField("CanExceedMaxGrade", BooleanType(), True),
    StructField("ExcludeFromFinalGradeCalc", BooleanType(), True),
    StructField("GradeSchemeId", LongType(), True),
    StructField("Weight", DoubleType(), True),
    StructField("NumLowestGradesToDrop", IntegerType(), True),
    StructField("NumHighestGradesToDrop", IntegerType(), True),
    StructField("WeightDistributionType", StringType(), True),
    StructField("CreatedDate", TimestampType(), True),
    StructField("ToolName", StringType(), True),
    StructField("AssociatedToolItemId", LongType(), True),
    StructField("LastModified", TimestampType(), True),
    StructField("ShortName", StringType(), True),
    StructField("GradeObjectTypeId", IntegerType(), True),
    StructField("SortOrder", IntegerType(), True),
    StructField("IsDeleted", BooleanType(), True),
    StructField("DeletedDate", TimestampType(), True),
    StructField("DeletedByUserId", LongType(), True),
    StructField("ResultId", LongType(), True),
    StructField("ToolId", IntegerType(), True),
    StructField("Version", LongType(), True),
])
