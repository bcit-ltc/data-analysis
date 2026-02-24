import sys

from pyspark.sql import SparkSession



DATASET_NAME = "discussions"
DATASET_TABLE = "discussionforums"


# - Condition: DeletedByUserId IS NOT NULL
#   Fields: DeletedByUserId
#   Description: Numeric identifier of the user who deleted the discussion forum (audit trail; pseudonymous personal data).
# - Condition: Name or Description contains personal names, student IDs, or other identifying text
#   Fields: Name, Description
#   Description: Forum titles and descriptions that may reference individuals if authored that way.



def main(input_base: str, output_base: str) -> None:
    spark = (
        SparkSession.builder
        .appName("discussionforums Deidentification")
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
        print("Usage: discussionsforum.py <input_base> <output_base>")
        sys.exit(1)

    input_base = sys.argv[1]
    output_base = sys.argv[2]
    main(input_base, output_base)
