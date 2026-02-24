import sys

from pyspark.sql import SparkSession



DATASET_NAME = "roledetails"
DATASET_TABLE = "roledetails"


# - Condition: DeletedBy IS NOT NULL
#   Fields: DeletedBy
#   Description: Numeric identifier of the user associated with role deletion or modification (audit trail; pseudonymous personal data).
# - Condition: RoleName, Description, ClassListRoleName, RoleAlias, or RoleCode contain personal names, student IDs, or other identifying text
#   Fields: RoleName, Description, ClassListRoleName, RoleAlias, RoleCode
#   Description: Free-text role metadata that could embed identifiers if roles are named after specific individuals.



def main(input_base: str, output_base: str) -> None:
    spark = (
        SparkSession.builder
        .appName("roledetails Deidentification")
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
        print("Usage: roledetails.py <input_base> <output_base>")
        sys.exit(1)

    input_base = sys.argv[1]
    output_base = sys.argv[2]
    main(input_base, output_base)
