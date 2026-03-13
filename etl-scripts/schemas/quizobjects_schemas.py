from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType, DoubleType
)

# Quiz Objects dataset (Quizzes)
# Docs: https://community.d2l.com/brightspace/kb/articles/4532-quizzes-data-sets
# Schema fields mirror the Brightspace Quiz Objects data set columns; see docs for field descriptions.
quiz_objects_schema = StructType([
    StructField("QuizId", LongType(), False),
    StructField("QuizName", StringType(), True),
    StructField("QuizDescription", StringType(), True),
    StructField("QuizCategory", StringType(), True),
    StructField("IsActive", BooleanType(), True),
    StructField("OrgUnitId", LongType(), False),
    StructField("StartDate", TimestampType(), True),
    StructField("EndDate", TimestampType(), True),
    StructField("DueDate", TimestampType(), True),
    StructField("CreationDate", TimestampType(), True),
    StructField("CreatedBy", LongType(), True),
    StructField("LastModified", TimestampType(), True),
    StructField("LastModifiedBy", LongType(), True),
    StructField("GradeObjectId", LongType(), True),
    StructField("OverallScoreCalculation", StringType(), True),
    StructField("QuizScoreDenominator", DoubleType(), True),
    StructField("HasPassword", BooleanType(), True),
    StructField("IPRestricted", BooleanType(), True),
    StructField("TimeLimit", IntegerType(), True),
    StructField("TimeLimitEnforced", BooleanType(), True),
    StructField("AttemptsAllowed", IntegerType(), True),
    StructField("PreventMovingBackwards", BooleanType(), True),
    StructField("AllowHints", BooleanType(), True),
    StructField("NotificationEmail", StringType(), True),
    StructField("DisablePagerAccess", BooleanType(), True),
    StructField("DisplayInCalendar", BooleanType(), True),
    StructField("IsAttemptRldb", BooleanType(), True),
    StructField("IsSubviewRldb", BooleanType(), True),
    StructField("SortOrder", IntegerType(), True),
    StructField("CategoryId", LongType(), True),
    StructField("ResultId", LongType(), True),
    StructField("IsRetakeIncorrectOnly", BooleanType(), True),
    StructField("PagingTypeId", IntegerType(), True),
    StructField("IsSynchronous", BooleanType(), True),
    StructField("DeductionPercentage", StringType(), True),
    StructField("AIStudySupport", BooleanType(), True),
    StructField("HideQuestionPoints", BooleanType(), True),
])
