#!/bin/bash
set -euo pipefail

if [[ "${RUN_ETL:-1}" != "1" ]]; then
  echo "[ETL] $(date -Iseconds) Skipping ETL (RUN_ETL != 1)"
  exit 0
fi

echo "[ETL] $(date -Iseconds) ETL scripts running ..."

# RELEASE_DATE="$RELEASE_DATE" #

COMMON="/opt/spark/bin/spark-submit --master spark://spark-master:7077"

# $COMMON etl-scripts/audiovideoprocessed.py data etl-output
# $COMMON etl-scripts/contentobjects.py data etl-output
# $COMMON etl-scripts/contentservice.py data etl-output
# $COMMON etl-scripts/discussionsforum.py data etl-output
# $COMMON etl-scripts/gradeobjects.py data etl-output
# $COMMON etl-scripts/organizationalunits.py data etl-output
$COMMON etl-scripts/quizobjects.py data etl-output
# $COMMON etl-scripts/releaseconditionsobjects.py data etl-output
# $COMMON etl-scripts/roledetails.py data etl-output
