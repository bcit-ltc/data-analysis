import sys

from pyspark.sql import SparkSession
from common import privacy_check


DATASET_NAME = "quizzes"
DATASET_TABLE = "quizobjects"


def read_input(spark: SparkSession, input_base: str):
    """Load the quizobjects dataset for deidentification.

    Implement this to read from the appropriate location under the ETL output
    directory (e.g., etl-output/{dataset}/{table}/data).
    """
    raise NotImplementedError("Implement dataset-specific read logic for quizobjects.")


def deidentify(df):
    """Apply quizobjects-specific deidentification transformations.

    This is where you should remove or transform direct identifiers and
    any other quiz-specific sensitive fields.
    """
    raise NotImplementedError("Implement quizobjects deidentification logic.")


def write_output(df, output_base: str):
    """Write the deidentified quizobjects dataset.

    Implement this to write to the desired deidentification output layout,
    typically under deidentification-output/.
    """
    raise NotImplementedError("Implement dataset-specific write logic for quizobjects.")


def main(input_base: str, output_base: str) -> None:
    spark = (
        SparkSession.builder
        .appName("Quiz Objects Deidentification")
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
