from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)

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
