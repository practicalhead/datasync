#!/bin/bash
#
# Run Hive Data Loading on Target Cluster
#
# This script executes the loading phase, which reads transferred data
# from staging and creates/updates Hive tables on the target cluster.
#
# Must be run on the TARGET Cloudera cluster.
#
# Usage:
#   ./run_loading.sh --config /path/to/config.yaml [options]
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
CONFIG_FILE=""
LOG_LEVEL="INFO"
DEPLOY_MODE="cluster"
WRITE_MODE="overwrite"
DRIVER_MEMORY="4g"
EXECUTOR_MEMORY="8g"
EXECUTOR_CORES="4"
NUM_EXECUTORS="10"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        --deploy-mode)
            DEPLOY_MODE="$2"
            shift 2
            ;;
        --mode)
            WRITE_MODE="$2"
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
        --help)
            echo "Usage: $0 --config <config.yaml> [options]"
            echo ""
            echo "Options:"
            echo "  --config          Path to job configuration YAML (required)"
            echo "  --log-level       Logging level (DEBUG, INFO, WARNING, ERROR)"
            echo "  --deploy-mode     Spark deploy mode (client, cluster)"
            echo "  --mode            Write mode (overwrite, append)"
            echo "  --driver-memory   Spark driver memory (e.g., 4g)"
            echo "  --executor-memory Spark executor memory (e.g., 8g)"
            echo "  --executor-cores  Spark executor cores (e.g., 4)"
            echo "  --num-executors   Number of Spark executors (e.g., 10)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [[ -z "$CONFIG_FILE" ]]; then
    echo "ERROR: --config is required"
    exit 1
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "ERROR: Config file not found: $CONFIG_FILE"
    exit 1
fi

# Extract job_id from config for naming
JOB_ID=$(grep 'job_id:' "$CONFIG_FILE" | head -1 | awk '{print $2}' | tr -d '"')
if [[ -z "$JOB_ID" ]]; then
    JOB_ID="datasync_$(date +%Y%m%d_%H%M%S)"
fi

echo "=========================================="
echo "Hive Data Loading Job"
echo "=========================================="
echo "Job ID: $JOB_ID"
echo "Config: $CONFIG_FILE"
echo "Log Level: $LOG_LEVEL"
echo "Write Mode: $WRITE_MODE"
echo "Deploy Mode: $DEPLOY_MODE"
echo "=========================================="

# Create archive of source code for distribution
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
ZIP_FILE="/tmp/datasync_${TIMESTAMP}.zip"

cd "$PROJECT_ROOT"
zip -r "$ZIP_FILE" src/ -x "*.pyc" -x "__pycache__/*" > /dev/null

echo "Source archive created: $ZIP_FILE"

# Run spark-submit
spark-submit \
    --master yarn \
    --deploy-mode "$DEPLOY_MODE" \
    --name "datasync_loading_${JOB_ID}" \
    --driver-memory "$DRIVER_MEMORY" \
    --executor-memory "$EXECUTOR_MEMORY" \
    --executor-cores "$EXECUTOR_CORES" \
    --num-executors "$NUM_EXECUTORS" \
    --conf spark.sql.hive.metastore.jars=builtin \
    --conf spark.yarn.appMasterEnv.PYTHONPATH=datasync.zip/src \
    --conf spark.executorEnv.PYTHONPATH=datasync.zip/src \
    --py-files "$ZIP_FILE" \
    "${PROJECT_ROOT}/src/loading/loader.py" \
    --config "$CONFIG_FILE" \
    --mode "$WRITE_MODE" \
    --log-level "$LOG_LEVEL"

EXIT_CODE=$?

# Cleanup
rm -f "$ZIP_FILE"

if [[ $EXIT_CODE -eq 0 ]]; then
    echo "=========================================="
    echo "Loading completed successfully"
    echo "=========================================="

    # Show loaded tables
    echo "Verifying loaded tables..."
    TARGET_DB=$(grep -A10 'tables:' "$CONFIG_FILE" | grep 'target_database:' | head -1 | awk '{print $2}' | tr -d '"')
    if [[ -n "$TARGET_DB" ]]; then
        beeline -e "SHOW TABLES IN $TARGET_DB" 2>/dev/null || true
    fi
else
    echo "=========================================="
    echo "Loading FAILED with exit code: $EXIT_CODE"
    echo "=========================================="
fi

exit $EXIT_CODE
