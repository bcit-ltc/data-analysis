import sys

from pyspark.sql import SparkSession



DATASET_NAME = "contentservice"
DATASET_TABLE = "audiovideoprocessed"


# - Condition: Any row where this dataset is joined to user-identifying tables using ContentId or RevisionId.
#   Fields: ContentId, RevisionId
#   Description: Technical content identifiers that become personal data when mapped to specific users or enrollments.



def main(input_base: str, output_base: str) -> None:
    spark = (
        SparkSession.builder
        .appName("audiovideoprocessed Deidentification")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = read_input(spark, input_base)
    deid_df = deidentify(df)
    write_output(deid_df, output_base)

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: quizobjects.py <input_base> <output_base>")
        sys.exit(1)

    input_base = sys.argv[1]
    output_base = sys.argv[2]
    main(input_base, output_base)