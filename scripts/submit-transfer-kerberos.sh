#!/bin/bash
#
# Submit Hive Cross-Cluster Transfer with Kerberos Authentication
#
# This script handles Kerberos authentication before submitting the job.
# Use this when both source and target clusters are Kerberos-secured.
#
# Requirements:
#   - Valid Kerberos keytab for the service account
#   - Cross-realm trust configured between clusters (if different realms)
#   - Network connectivity to both cluster KDCs

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Kerberos settings
KERBEROS_PRINCIPAL=""
KERBEROS_KEYTAB=""
KERBEROS_REALM=""

# Source cluster HiveServer2 principal (for JDBC authentication)
SOURCE_HS2_PRINCIPAL=""

# Default configurations
JAR_PATH="${PROJECT_DIR}/target/scala-2.12/hive-cross-cluster-transfer-1.0.0.jar"
CONFIG_PATH=""
DRIVER_MEMORY="4g"
EXECUTOR_MEMORY="8g"
NUM_EXECUTORS="10"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --config)
      CONFIG_PATH="$2"
      shift 2
      ;;
    --principal)
      KERBEROS_PRINCIPAL="$2"
      shift 2
      ;;
    --keytab)
      KERBEROS_KEYTAB="$2"
      shift 2
      ;;
    --source-hs2-principal)
      SOURCE_HS2_PRINCIPAL="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

# Validate Kerberos settings
if [[ -z "$KERBEROS_PRINCIPAL" ]] || [[ -z "$KERBEROS_KEYTAB" ]]; then
  echo "Error: Kerberos authentication required"
  echo "Usage: $0 --config <config> --principal <principal> --keytab <keytab>"
  exit 1
fi

if [[ ! -f "$KERBEROS_KEYTAB" ]]; then
  echo "Error: Keytab file not found: $KERBEROS_KEYTAB"
  exit 1
fi

echo "========================================"
echo "Kerberos Authentication"
echo "========================================"
echo "Principal: $KERBEROS_PRINCIPAL"
echo "Keytab: $KERBEROS_KEYTAB"

# Authenticate with Kerberos
kinit -kt "$KERBEROS_KEYTAB" "$KERBEROS_PRINCIPAL"
if [[ $? -ne 0 ]]; then
  echo "Error: Kerberos authentication failed"
  exit 1
fi

echo "Kerberos authentication successful"
klist

echo "========================================"
echo "Submitting Transfer Job"
echo "========================================"

# Build JDBC URL with Kerberos auth if source principal provided
if [[ -n "$SOURCE_HS2_PRINCIPAL" ]]; then
  echo "Source HS2 Principal: $SOURCE_HS2_PRINCIPAL"
fi

# Submit with Kerberos-specific Spark settings
spark-submit \
  --class com.datasync.HiveCrossClusterTransfer \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory "$DRIVER_MEMORY" \
  --executor-memory "$EXECUTOR_MEMORY" \
  --num-executors "$NUM_EXECUTORS" \
  --principal "$KERBEROS_PRINCIPAL" \
  --keytab "$KERBEROS_KEYTAB" \
  --conf "spark.yarn.keytab=$KERBEROS_KEYTAB" \
  --conf "spark.yarn.principal=$KERBEROS_PRINCIPAL" \
  --conf "spark.kerberos.keytab=$KERBEROS_KEYTAB" \
  --conf "spark.kerberos.principal=$KERBEROS_PRINCIPAL" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.network.timeout=600s" \
  --files "$CONFIG_PATH" \
  "$JAR_PATH" \
  --config "$(basename "$CONFIG_PATH")"

echo "Transfer job submitted successfully"
