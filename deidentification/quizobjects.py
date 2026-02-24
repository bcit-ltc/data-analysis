import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

DATASET_NAME = "quizzes"
DATASET_TABLE = "quizobjects"

# - Condition: NotificationEmail IS NOT NULL
#   Fields: NotificationEmail
#   Description: Direct email address for quiz notifications; clear PII.
# - Condition: CreatedBy IS NOT NULL
#   Fields: CreatedBy
#   Description: Numeric identifier of the user who created the quiz.
# - Condition: LastModifiedBy IS NOT NULL
#   Fields: LastModifiedBy
#   Description: Numeric identifier of the user who last modified the quiz.
# - Condition: QuizName, QuizDescription, QuizCategory, or OverallScoreCalculation contain personal names, student IDs, or other identifying text
#   Fields: QuizName, QuizDescription, QuizCategory, OverallScoreCalculation
#   Description: Free-text metadata/instructions that can embed personal identifiers depending on authoring practices.


def read_input(spark, input_base):
    return spark.read.parquet(f"{input_base}/{DATASET_NAME}/{DATASET_TABLE}")


def deidentify(df):
    # Deidentification logic goes here
    # For now, just return the original dataframe
    return df


def write_output(df, output_base):
    df.write.parquet(f"{output_base}/{DATASET_NAME}/{DATASET_TABLE}")


def main(input_base: str, output_base: str) -> None:
    spark = (
        SparkSession.builder
        .appName("quizobjects Deidentification")
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
