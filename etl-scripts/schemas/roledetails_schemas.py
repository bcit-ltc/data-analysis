from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, LongType, TimestampType
)

# Role Details dataset
# Docs: https://community.d2l.com/brightspace/kb/articles/4534-role-details-data-sets
# Schema fields mirror the Brightspace Role Details data set columns; see docs for field descriptions.
role_details_schema = StructType([
    StructField("OrgUnitId", IntegerType(), False),
    StructField("RoleId", IntegerType(), False),
    StructField("RoleName", StringType(), False),
    StructField("Description", StringType(), True),
    StructField("IsCascading", BooleanType(), False),
    StructField("InClassList", BooleanType(), False),
    StructField("ClassListRoleName", StringType(), True),
    StructField("ClassListShowGroups", BooleanType(), False),
    StructField("ClassListShowSections", BooleanType(), False),
    StructField("ClassListDisplayRole", BooleanType(), False),
    StructField("AccessInactiveCO", BooleanType(), False),
    StructField("HasSpecialAccess", BooleanType(), False),
    StructField("AddToCourseOfferingGroups", BooleanType(), False),
    StructField("CanBeAutoEnrolledIntoGroups", BooleanType(), False),
    StructField("AddToCourseOfferingSections", BooleanType(), False),
    StructField("CanBeAutoEnrolledIntoSections", BooleanType(), False),
    StructField("AccessPastCourses", BooleanType(), False),
    StructField("AccessFutureCourses", BooleanType(), False),
    StructField("SortOrder", IntegerType(), False),
    StructField("ShowInContent", BooleanType(), False),
    StructField("ShowInDiscussionAssess", BooleanType(), False),
    StructField("ShowInDiscussionStats", BooleanType(), False),
    StructField("ShowInGrades", BooleanType(), False),
    StructField("ShowInAttendance", BooleanType(), False),
    StructField("AllowSelfEnrollInGroups", BooleanType(), False),
    StructField("ShowInRegistration", BooleanType(), False),
    StructField("ShowInUserProgress", BooleanType(), False),
    StructField("RoleAlias", StringType(), True),
    StructField("RoleCode", StringType(), True),
    StructField("LastModifiedDate", TimestampType(), True),
    StructField("DeletedBy", IntegerType(), True),
])
