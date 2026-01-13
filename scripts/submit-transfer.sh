#!/bin/bash
#
# Submit Hive Cross-Cluster Transfer Job
#
# This script submits the transfer job to run on the TARGET cluster.
# The Spark application reads from source via JDBC and writes to target
# using native Spark-Hive integration.
#
# Usage:
#   ./submit-transfer.sh --config /path/to/transfer-config.conf
#
# Requirements:
#   - Run on TARGET cluster (where data will be written)
#   - HiveServer2 on source cluster must be accessible from target
#   - Appropriate network connectivity between clusters
#   - Kerberos cross-realm trust if both clusters are secured

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Default configurations
JAR_PATH="${PROJECT_DIR}/target/scala-2.12/hive-cross-cluster-transfer-1.0.0.jar"
CONFIG_PATH=""
DRIVER_MEMORY="4g"
EXECUTOR_MEMORY="8g"
EXECUTOR_CORES="4"
NUM_EXECUTORS="10"
QUEUE="default"
DEPLOY_MODE="cluster"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --config)
      CONFIG_PATH="$2"
      shift 2
      ;;
    --jar)
      JAR_PATH="$2"
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
    --executor-cores)
      EXECUTOR_CORES="$2"
      shift 2
      ;;
    --num-executors)
      NUM_EXECUTORS="$2"
      shift 2
      ;;
    --queue)
      QUEUE="$2"
      shift 2
      ;;
    --deploy-mode)
      DEPLOY_MODE="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Validate required arguments
if [[ -z "$CONFIG_PATH" ]]; then
  echo "Error: --config is required"
  echo "Usage: $0 --config /path/to/transfer-config.conf"
  exit 1
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Error: Config file not found: $CONFIG_PATH"
  exit 1
fi

if [[ ! -f "$JAR_PATH" ]]; then
  echo "Error: JAR file not found: $JAR_PATH"
  echo "Build the project first: cd $PROJECT_DIR && sbt assembly"
  exit 1
fi

echo "========================================"
echo "Hive Cross-Cluster Transfer"
echo "========================================"
echo "Config: $CONFIG_PATH"
echo "JAR: $JAR_PATH"
echo "Deploy Mode: $DEPLOY_MODE"
echo "Driver Memory: $DRIVER_MEMORY"
echo "Executor Memory: $EXECUTOR_MEMORY"
echo "Executors: $NUM_EXECUTORS x $EXECUTOR_CORES cores"
echo "Queue: $QUEUE"
echo "========================================"

# Submit the Spark job
spark-submit \
  --class com.datasync.HiveCrossClusterTransfer \
  --master yarn \
  --deploy-mode "$DEPLOY_MODE" \
  --driver-memory "$DRIVER_MEMORY" \
  --executor-memory "$EXECUTOR_MEMORY" \
  --executor-cores "$EXECUTOR_CORES" \
  --num-executors "$NUM_EXECUTORS" \
  --queue "$QUEUE" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  --conf "spark.dynamicAllocation.enabled=true" \
  --conf "spark.dynamicAllocation.minExecutors=2" \
  --conf "spark.dynamicAllocation.maxExecutors=$NUM_EXECUTORS" \
  --conf "spark.yarn.maxAppAttempts=2" \
  --conf "spark.network.timeout=600s" \
  --conf "spark.sql.broadcastTimeout=600" \
  --files "$CONFIG_PATH" \
  "$JAR_PATH" \
  --config "$(basename "$CONFIG_PATH")"

echo "Transfer job submitted successfully"
