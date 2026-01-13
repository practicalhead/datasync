package com.datasync

import com.datasync.config.TransferConfig
import com.datasync.transfer.CrossClusterTransferExecutor
import com.datasync.utils.KerberosAuth
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Main entry point for Hive Cross-Cluster Data Transfer.
 *
 * This application transfers data from Hive tables in a source Cloudera cluster
 * to a target Cloudera cluster, with support for:
 *
 * - Row-level filtering (SQL WHERE conditions)
 * - Column selection
 * - Partitioned tables
 * - Kerberos authentication
 * - Audit logging
 *
 * Architecture:
 * - Runs on TARGET cluster (SparkContext bound to target)
 * - Reads from SOURCE cluster via JDBC (HiveServer2)
 * - Writes to TARGET cluster via native Spark-Hive integration
 *
 * This approach eliminates the need for:
 * - distcp or manual file transfers
 * - Shared storage between clusters
 * - Multiple SparkContexts
 *
 * Usage:
 *   spark-submit \
 *     --class com.datasync.HiveCrossClusterTransfer \
 *     --master yarn \
 *     --deploy-mode cluster \
 *     hive-cross-cluster-transfer-1.0.0.jar \
 *     --config /path/to/transfer-config.conf
 */
object HiveCrossClusterTransfer {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val parsedArgs = parseArgs(args)

    // Load configuration
    val configPath = parsedArgs.getOrElse("config",
      throw new IllegalArgumentException("--config argument is required"))
    val config = TransferConfig.fromFile(configPath)

    logger.info("=" * 60)
    logger.info("Hive Cross-Cluster Transfer")
    logger.info("=" * 60)
    logger.info(s"Job ID: ${config.jobId}")
    logger.info(s"Source: ${config.sourceCluster.name} (${config.sourceCluster.hiveDatabase})")
    logger.info(s"Target: ${config.targetCluster.name} (${config.targetCluster.hiveDatabase})")
    logger.info(s"Tables: ${config.tables.map(_.sourceTable).mkString(", ")}")
    logger.info("=" * 60)

    // Authenticate if Kerberos is enabled
    if (config.sourceCluster.kerberosEnabled || config.targetCluster.kerberosEnabled) {
      authenticateKerberos(config)
    }

    // Create Spark session on TARGET cluster with Hive support
    val spark = createSparkSession(config)

    try {
      // Execute transfers
      val executor = new CrossClusterTransferExecutor(spark, config)
      val results = executor.executeAll()

      // Exit with appropriate code
      val hasFailures = results.values.exists(!_.success)
      if (hasFailures) {
        logger.error("Transfer completed with failures")
        System.exit(1)
      } else {
        logger.info("Transfer completed successfully")
        System.exit(0)
      }
    } finally {
      spark.stop()
    }
  }

  /**
   * Create SparkSession with Hive support for target cluster.
   */
  private def createSparkSession(config: com.datasync.config.TransferJobConfig): SparkSession = {
    val builder = SparkSession.builder()
      .appName(s"HiveCrossClusterTransfer-${config.jobId}")
      .enableHiveSupport()
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.hive.convertMetastoreParquet", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

    // Configure for better JDBC read performance
    builder.config("spark.sql.shuffle.partitions", config.parallelism.toString)

    builder.getOrCreate()
  }

  /**
   * Authenticate with Kerberos using the configured credentials.
   */
  private def authenticateKerberos(config: com.datasync.config.TransferJobConfig): Unit = {
    // Authenticate for target cluster (where Spark runs)
    if (config.targetCluster.kerberosEnabled) {
      val principal = config.targetCluster.kerberosPrincipal.getOrElse(
        throw new IllegalArgumentException("Kerberos principal required for target cluster"))
      val keytab = config.targetCluster.kerberosKeytab.getOrElse(
        throw new IllegalArgumentException("Kerberos keytab required for target cluster"))

      KerberosAuth.authenticateWithKeytab(principal, keytab)
    }

    // Configure cross-realm trust if source uses different realm
    if (config.sourceCluster.kerberosEnabled && config.targetCluster.kerberosEnabled) {
      val sourceRealm = config.sourceCluster.kerberosPrincipal.map(_.split("@").last)
      val targetRealm = config.targetCluster.kerberosPrincipal.map(_.split("@").last)

      if (sourceRealm != targetRealm) {
        logger.info("Configuring cross-realm Kerberos trust")
        KerberosAuth.configureCrossRealmTrust(sourceRealm.get, targetRealm.get)
      }
    }
  }

  /**
   * Parse command line arguments.
   */
  private def parseArgs(args: Array[String]): Map[String, String] = {
    args.sliding(2, 2).collect {
      case Array(key, value) if key.startsWith("--") =>
        key.stripPrefix("--") -> value
    }.toMap
  }
}
