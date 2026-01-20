#!/bin/bash
set -euo pipefail

if [[ "${RUN_ETL:-1}" != "1" ]]; then
  echo "[ETL] $(date -Iseconds) Skipping ETL (RUN_ETL != 1)"
  exit 0
fi

echo "[ETL] $(date -Iseconds) ETL scripts running ..."

RELEASE_DATE="$RELEASE_DATE" # release date for the static files from GTFS translink data

COMMON="/opt/spark/bin/spark-submit --master spark://spark-master:7077"

$COMMON etl-scripts/calendar_etl.py "$RELEASE_DATE" translink-data
$COMMON etl-scripts/calendar_dates_etl.py "$RELEASE_DATE" translink-data
$COMMON etl-scripts/routes_etl.py "$RELEASE_DATE" translink-data
$COMMON etl-scripts/stops_etl.py "$RELEASE_DATE" translink-data
$COMMON etl-scripts/stop_times_etl.py "$RELEASE_DATE" translink-data
$COMMON etl-scripts/shapes_etl.py "$RELEASE_DATE" translink-data
$COMMON etl-scripts/transfers_etl.py "$RELEASE_DATE" translink-data
$COMMON etl-scripts/trips_etl.py "$RELEASE_DATE" translink-data


# $COMMON etl-scripts/historical_live_etl.py
  
# add more etl scripts here
# $COMMON etl-scripts/more_scripts.py "$RELEASE_DATE" translink-data