from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)

# Audio Video Processed dataset (ContentService)
# Docs: https://community.d2l.com/brightspace/kb/articles/22812-content-service-data-sets
# Schema fields mirror the Brightspace Audio Video Processed data set columns; see docs for field descriptions.
audio_video_processed_schema = StructType([
    StructField("ContentId", StringType(), False),
    StructField("RevisionId", StringType(), True),
    StructField("RevisionNumber", IntegerType(), True),
    StructField("Type", StringType(), True),
    StructField("Source", StringType(), True),
    StructField("RevisionSize", LongType(), True),
    StructField("Duration", IntegerType(), True),
    StructField("RequiredTranscoding", BooleanType(), True),
    StructField("RequiredTranscribing", BooleanType(), True),
    StructField("LastModified", TimestampType(), True),
])
