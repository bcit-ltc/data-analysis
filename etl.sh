#!/bin/bash
set -euo pipefail

if [[ "${RUN_ETL:-1}" != "1" ]]; then
  echo "[ETL] $(date -Iseconds) Skipping ETL (RUN_ETL != 1)"
  exit 0
fi

echo "[ETL] $(date -Iseconds) ETL scripts running ..."

# RELEASE_DATE="$RELEASE_DATE" #

COMMON="/opt/spark/bin/spark-submit --master spark://spark-master:7077"

# ======== ETL scripts

$COMMON etl-scripts/audiovideoprocessed.py data etl-output
$COMMON etl-scripts/contentobjects.py data etl-output
$COMMON etl-scripts/discussionsforum.py data etl-output
$COMMON etl-scripts/gradeobjects.py data etl-output
$COMMON etl-scripts/organizationalunits.py data etl-output
$COMMON etl-scripts/quizobjects.py data etl-output
$COMMON etl-scripts/releaseconditionsobjects.py data etl-output
$COMMON etl-scripts/roledetails.py data etl-output

# ======== De-Identification and final csv

$COMMON deidentification/audiovideoprocessed.py etl-output output
$COMMON deidentification/contentobjects.py etl-output output
$COMMON deidentification/discussionsforum.py etl-output output
$COMMON deidentification/gradeobjects.py etl-output output
$COMMON deidentification/organizationalunits.py etl-output output
$COMMON deidentification/quizobjects.py etl-output output
$COMMON deidentification/releaseconditionsobjects.py etl-output output
$COMMON deidentification/roledetails.py etl-output output