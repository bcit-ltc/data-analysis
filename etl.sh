#!/bin/bash
set -euo pipefail

if [[ "${RUN_ETL:-1}" != "1" ]]; then
  echo "[ETL] $(date -Iseconds) Skipping ETL (RUN_ETL != 1)"
  exit 0
fi

echo "[ETL] $(date -Iseconds) ETL scripts running ..."

# RELEASE_DATE="$RELEASE_DATE" #

COMMON="/opt/spark/bin/spark-submit --master spark://spark-master:7077"

# Preprocess DiscussionForums CSV so quoted fields (commas/newlines) don't misalign columns
if [[ -f data/DiscussionsForum/DiscussionForums.csv ]]; then
  echo "[ETL] Preprocessing DiscussionForums.csv ..."
  python3 etl-scripts/preprocess_csv_quoted.py \
    data/DiscussionsForum/DiscussionForums.csv \
    data/DiscussionsForum/DiscussionForums_preprocessed.csv
fi

# $COMMON etl-scripts/audiovideoprocessed.py data output
# $COMMON etl-scripts/contentservice.py data output
$COMMON etl-scripts/discussionsforum.py data output
# $COMMON etl-scripts/gradeobjects.py data output
# $COMMON etl-scripts/organizationalunits.py data output
# $COMMON etl-scripts/releaseconditionsobjects.py data output
# $COMMON etl-scripts/roledetails.py data output
