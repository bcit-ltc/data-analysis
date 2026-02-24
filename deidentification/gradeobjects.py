import sys

from pyspark.sql import SparkSession



DATASET_NAME = "grades"
DATASET_TABLE = "gradeobjects"


# - Condition: DeletedByUserId IS NOT NULL
#   Fields: DeletedByUserId
#   Description: Numeric identifier of the user who deleted the grade object (audit trail; pseudonymous personal data).
# - Condition: Name or ShortName contains personal names, student IDs, or other identifying text
#   Fields: Name, ShortName
#   Description: Free-text labels for grade items that can include identifiers if authored that way.



def main(input_base: str, output_base: str) -> None:
    spark = (
        SparkSession.builder
        .appName("gradeobjects Deidentification")
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
        print("Usage: gradeobjects.py <input_base> <output_base>")
        sys.exit(1)

    input_base = sys.argv[1]
    output_base = sys.argv[2]
    main(input_base, output_base)
