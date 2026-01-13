#!/bin/bash
#
# Run Secure Data Transfer Between Clusters
#
# This script executes DistCp or WebHDFS transfer to move extracted data
# from the source cluster staging area to the target cluster.
#
# Can be run from either cluster with appropriate Kerberos credentials.
#
# Usage:
#   ./run_transfer.sh --config /path/to/config.yaml [options]
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
CONFIG_FILE=""
TRANSFER_METHOD="distcp"
BANDWIDTH_LIMIT="100"
NUM_MAPS="10"
SKIP_CHECKSUM="false"
DRY_RUN="false"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --method)
            TRANSFER_METHOD="$2"
            shift 2
            ;;
        --bandwidth)
            BANDWIDTH_LIMIT="$2"
            shift 2
            ;;
        --maps)
            NUM_MAPS="$2"
            shift 2
            ;;
        --skip-checksum)
            SKIP_CHECKSUM="true"
            shift
            ;;
        --dry-run)
            DRY_RUN="true"
            shift
            ;;
        --help)
            echo "Usage: $0 --config <config.yaml> [options]"
            echo ""
            echo "Options:"
            echo "  --config        Path to job configuration YAML (required)"
            echo "  --method        Transfer method (distcp, webhdfs)"
            echo "  --bandwidth     Bandwidth limit in MB/s (default: 100)"
            echo "  --maps          Number of parallel maps for DistCp (default: 10)"
            echo "  --skip-checksum Skip checksum verification"
            echo "  --dry-run       Show what would be transferred without executing"
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

# Parse configuration
JOB_ID=$(grep 'job_id:' "$CONFIG_FILE" | head -1 | awk '{print $2}' | tr -d '"')
SOURCE_NAMENODE=$(grep -A5 'source_cluster:' "$CONFIG_FILE" | grep 'hdfs_namenode:' | awk '{print $2}' | tr -d '"')
TARGET_NAMENODE=$(grep -A5 'target_cluster:' "$CONFIG_FILE" | grep 'hdfs_namenode:' | awk '{print $2}' | tr -d '"')
STAGING_PATH=$(grep -A10 'source_cluster:' "$CONFIG_FILE" | grep 'staging_path:' | head -1 | awk '{print $2}' | tr -d '"')

if [[ -z "$STAGING_PATH" ]]; then
    STAGING_PATH="/tmp/datasync/staging"
fi

SOURCE_PATH="${SOURCE_NAMENODE}${STAGING_PATH}/${JOB_ID}"
TARGET_PATH="${TARGET_NAMENODE}${STAGING_PATH}/${JOB_ID}"

echo "=========================================="
echo "Hive Data Transfer Job"
echo "=========================================="
echo "Job ID: $JOB_ID"
echo "Method: $TRANSFER_METHOD"
echo "Source: $SOURCE_PATH"
echo "Target: $TARGET_PATH"
echo "Bandwidth: ${BANDWIDTH_LIMIT} MB/s"
echo "Maps: $NUM_MAPS"
echo "=========================================="

if [[ "$DRY_RUN" == "true" ]]; then
    echo "DRY RUN - No data will be transferred"
    echo ""
fi

# Build DistCp command
DISTCP_CMD="hadoop distcp"
DISTCP_CMD+=" -m $NUM_MAPS"
DISTCP_CMD+=" -bandwidth $BANDWIDTH_LIMIT"
DISTCP_CMD+=" -p"  # Preserve permissions
DISTCP_CMD+=" -update"
DISTCP_CMD+=" -overwrite"

if [[ "$SKIP_CHECKSUM" == "true" ]]; then
    DISTCP_CMD+=" -skipcrccheck"
fi

DISTCP_CMD+=" $SOURCE_PATH $TARGET_PATH"

echo "Command: $DISTCP_CMD"
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
    echo "Dry run complete. Execute without --dry-run to transfer data."
    exit 0
fi

# Execute transfer
START_TIME=$(date +%s)

if [[ "$TRANSFER_METHOD" == "distcp" ]]; then
    echo "Starting DistCp transfer..."
    eval "$DISTCP_CMD"
    EXIT_CODE=$?
elif [[ "$TRANSFER_METHOD" == "webhdfs" ]]; then
    echo "WebHDFS transfer not implemented in shell script."
    echo "Use the Python orchestrator for WebHDFS transfers."
    exit 1
else
    echo "Unknown transfer method: $TRANSFER_METHOD"
    exit 1
fi

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

if [[ $EXIT_CODE -eq 0 ]]; then
    echo "=========================================="
    echo "Transfer completed successfully"
    echo "Duration: ${DURATION}s"
    echo "=========================================="

    # Verify transfer
    echo "Verifying transfer..."
    hdfs dfs -ls "$TARGET_PATH" | head -20
else
    echo "=========================================="
    echo "Transfer FAILED with exit code: $EXIT_CODE"
    echo "Duration: ${DURATION}s"
    echo "=========================================="
fi

exit $EXIT_CODE
