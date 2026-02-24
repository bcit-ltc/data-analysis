import sys

from pyspark.sql import SparkSession



DATASET_NAME = "organizationalunits"
DATASET_TABLE = "organizationalunits"

# PII risks: 
# - Condition: None specifically identified from schema alone; table primarily describes courses/org units.
#   Fields: Name, Code, Organization
#   Description: Organizational labels; generally not PII, but could reference individuals if org-unit naming conventions include person names.



def main(input_base: str, output_base: str) -> None:
    spark = (
        SparkSession.builder
        .appName("organizationalunits Deidentification")
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
        print("Usage: organizationalunits.py <input_base> <output_base>")
        sys.exit(1)

    input_base = sys.argv[1]
    output_base = sys.argv[2]
    main(input_base, output_base)
