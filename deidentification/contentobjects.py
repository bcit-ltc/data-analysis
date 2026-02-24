import sys

from pyspark.sql import SparkSession



DATASET_NAME = "contentdata"
DATASET_TABLE = "contentobjects"


# - Condition: CreatedBy IS NOT NULL
#   Fields: CreatedBy
#   Description: Numeric identifier of the user who created the content object (pseudonymous personal data when user lookup exists).
# - Condition: LastModifiedBy IS NOT NULL
#   Fields: LastModifiedBy
#   Description: Numeric identifier of the user who last modified the content object.
# - Condition: DeletedBy IS NOT NULL
#   Fields: DeletedBy
#   Description: Numeric identifier of the user who deleted the content object.
# - Condition: Title or Location contains personal names, student IDs, or other identifying text
#   Fields: Title, Location
#   Description: Free-text metadata that may embed names, IDs, or other identifiers depending on how instructors author content.



def main(input_base: str, output_base: str) -> None:
    spark = (
        SparkSession.builder
        .appName("contentobjects Deidentification")
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
        print("Usage: contentobjects.py <input_base> <output_base>")
        sys.exit(1)

    input_base = sys.argv[1]
    output_base = sys.argv[2]
    main(input_base, output_base)
