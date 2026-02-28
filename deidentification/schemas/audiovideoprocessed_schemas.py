from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType, TimestampType, LongType
)

audio_video_processed_schema = StructType([
    StructField("content_id", StringType(), False),
    StructField("revision_id", StringType(), True),
    StructField("revision_number", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("revision_size", LongType(), True),
    StructField("duration", IntegerType(), True),
    StructField("required_transcoding", BooleanType(), True),
    StructField("required_transcribing", BooleanType(), True),
    StructField("last_modified", TimestampType(), True),
])
