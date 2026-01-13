#!/bin/bash
#
# Simple Single-Table Transfer Script
#
# Quick way to transfer a single table with filtering without
# needing a full configuration file.
#
# Usage:
#   ./simple-transfer.sh \
#     --source-jdbc "jdbc:hive2://source-host:10000/default" \
#     --source-table "source_db.my_table" \
#     --target-table "target_db.my_table" \
#     --columns "col1,col2,col3" \
#     --filter "date_col >= '2024-01-01'"

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
JAR_PATH="${PROJECT_DIR}/target/scala-2.12/hive-cross-cluster-transfer-1.0.0.jar"

# Default resource settings
DRIVER_MEMORY="2g"
EXECUTOR_MEMORY="4g"
NUM_EXECUTORS="5"

# Collect arguments for the Spark application
APP_ARGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    --source-jdbc|--source-table|--target-table|--columns|--filter|--partition-by|--write-mode)
      APP_ARGS+=("$1" "$2")
      shift 2
      ;;
    --driver-memory)
      DRIVER_MEMORY="$2"
      shift 2
      ;;
    --executor-memory)
      EXECUTOR_MEMORY="$2"
      shift 2
      ;;
    --num-executors)
      NUM_EXECUTORS="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

echo "Starting simple table transfer..."
echo "Arguments: ${APP_ARGS[*]}"

spark-submit \
  --class com.datasync.SimpleTransfer \
  --master yarn \
  --deploy-mode client \
  --driver-memory "$DRIVER_MEMORY" \
  --executor-memory "$EXECUTOR_MEMORY" \
  --num-executors "$NUM_EXECUTORS" \
  --conf "spark.sql.adaptive.enabled=true" \
  "$JAR_PATH" \
  "${APP_ARGS[@]}"

echo "Transfer completed"
