#!/bin/bash
#
# Run Complete Hive Data Transfer Workflow
#
# This script orchestrates the complete data transfer workflow:
# 1. Extract data from source cluster with filtering
# 2. Transfer data securely between clusters
# 3. Load data into target cluster Hive tables
#
# For cross-cluster execution, use the Python orchestrator directly
# or run individual phase scripts on each cluster.
#
# Usage:
#   ./run_workflow.sh --config /path/to/config.yaml [options]
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
CONFIG_FILE=""
START_PHASE="extraction"
LOG_LEVEL="INFO"
RESUME="false"
EXECUTION_MODE="spark-submit"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --start-from)
            START_PHASE="$2"
            shift 2
            ;;
        --log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        --resume)
            RESUME="true"
            shift
            ;;
        --execution-mode)
            EXECUTION_MODE="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 --config <config.yaml> [options]"
            echo ""
            echo "Options:"
            echo "  --config          Path to job configuration YAML (required)"
            echo "  --start-from      Phase to start from (extraction, transfer, loading)"
            echo "  --log-level       Logging level (DEBUG, INFO, WARNING, ERROR)"
            echo "  --resume          Resume from checkpoint if available"
            echo "  --execution-mode  How to run Spark (local, yarn, spark-submit)"
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

# Extract job info
JOB_ID=$(grep 'job_id:' "$CONFIG_FILE" | head -1 | awk '{print $2}' | tr -d '"')
JOB_NAME=$(grep 'job_name:' "$CONFIG_FILE" | head -1 | awk -F': ' '{print $2}' | tr -d '"')

echo "============================================================"
echo "Hive Data Transfer Workflow"
echo "============================================================"
echo "Job ID:     $JOB_ID"
echo "Job Name:   $JOB_NAME"
echo "Config:     $CONFIG_FILE"
echo "Start From: $START_PHASE"
echo "Log Level:  $LOG_LEVEL"
echo "============================================================"
echo ""

WORKFLOW_START=$(date +%s)

# Function to run a phase
run_phase() {
    local PHASE=$1
    local SCRIPT=$2

    echo "------------------------------------------------------------"
    echo "Phase: $PHASE"
    echo "Started: $(date)"
    echo "------------------------------------------------------------"

    PHASE_START=$(date +%s)

    if $SCRIPT; then
        PHASE_END=$(date +%s)
        PHASE_DURATION=$((PHASE_END - PHASE_START))
        echo "$PHASE completed in ${PHASE_DURATION}s"
        return 0
    else
        echo "$PHASE FAILED"
        return 1
    fi
}

# Determine which phases to run
RUN_EXTRACTION=false
RUN_TRANSFER=false
RUN_LOADING=false

case $START_PHASE in
    extraction)
        RUN_EXTRACTION=true
        RUN_TRANSFER=true
        RUN_LOADING=true
        ;;
    transfer)
        RUN_TRANSFER=true
        RUN_LOADING=true
        ;;
    loading)
        RUN_LOADING=true
        ;;
    *)
        echo "Unknown phase: $START_PHASE"
        exit 1
        ;;
esac

# Run Python orchestrator for coordinated execution
echo "Running workflow via Python orchestrator..."
echo ""

cd "$PROJECT_ROOT"

PYTHON_CMD="python src/orchestration/workflow.py"
PYTHON_CMD+=" --config $CONFIG_FILE"
PYTHON_CMD+=" --log-level $LOG_LEVEL"
PYTHON_CMD+=" --execution-mode $EXECUTION_MODE"

if [[ "$RESUME" == "true" ]]; then
    PYTHON_CMD+=" --resume"
fi

case $START_PHASE in
    extraction)
        PYTHON_CMD+=" --phase full"
        ;;
    transfer)
        PYTHON_CMD+=" --phase transfer"
        ;;
    loading)
        PYTHON_CMD+=" --phase loading"
        ;;
esac

echo "Command: $PYTHON_CMD"
echo ""

eval "$PYTHON_CMD"
EXIT_CODE=$?

WORKFLOW_END=$(date +%s)
WORKFLOW_DURATION=$((WORKFLOW_END - WORKFLOW_START))

echo ""
echo "============================================================"
if [[ $EXIT_CODE -eq 0 ]]; then
    echo "Workflow COMPLETED successfully"
else
    echo "Workflow FAILED with exit code: $EXIT_CODE"
fi
echo "Total Duration: ${WORKFLOW_DURATION}s"
echo "============================================================"

exit $EXIT_CODE
