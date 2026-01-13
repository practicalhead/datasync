package com.datasync.transfer

import com.datasync.config.{TableTransferConfig, TransferJobConfig}
import com.datasync.utils.{AuditLogger, TransferMetrics}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
 * Executes cross-cluster Hive data transfers.
 *
 * Architecture:
 * - Runs on TARGET cluster (SparkContext bound to target)
 * - Reads from SOURCE cluster via JDBC (HiveServer2)
 * - Writes to TARGET cluster via native Spark-Hive integration
 *
 * This design eliminates the need for:
 * - distcp or manual file transfers
 * - Shared storage between clusters
 * - Multiple SparkContexts
 *
 * Data Flow:
 * Source HiveServer2 --[JDBC]--> Spark Executors --[Native]--> Target HDFS/Hive
 */
class CrossClusterTransferExecutor(
  spark: SparkSession,
  config: TransferJobConfig
) {

  private val logger = LoggerFactory.getLogger(classOf[CrossClusterTransferExecutor])
  private val sourceReader = new HiveJdbcReader(spark, config.sourceCluster)
  private val targetWriter = new HiveTargetWriter(spark, config.targetCluster)
  private val auditLogger = if (config.enableAudit) Some(new AuditLogger(spark, config.jobId)) else None

  /**
   * Execute all table transfers defined in the configuration.
   *
   * @return Map of table names to transfer results
   */
  def executeAll(): Map[String, TransferResult] = {
    logger.info(s"Starting cross-cluster transfer job: ${config.jobId}")
    logger.info(s"Source cluster: ${config.sourceCluster.name}")
    logger.info(s"Target cluster: ${config.targetCluster.name}")
    logger.info(s"Tables to transfer: ${config.tables.size}")

    val results = config.tables.map { tableConfig =>
      val tableName = tableConfig.sourceTable
      val result = executeTableTransfer(tableConfig)
      tableName -> result
    }.toMap

    logSummary(results)
    results
  }

  /**
   * Execute transfer for a single table.
   */
  def executeTableTransfer(tableConfig: TableTransferConfig): TransferResult = {
    val startTime = System.currentTimeMillis()
    val metrics = TransferMetrics(tableConfig.sourceTable)

    logger.info(s"Starting transfer: ${tableConfig.sourceTable} -> ${tableConfig.targetTable}")
    logger.info(s"Columns: ${tableConfig.columns.mkString(", ")}")
    logger.info(s"Filter: ${tableConfig.filterCondition.getOrElse("none")}")

    Try {
      // Read from source via JDBC
      val sourceData = readSourceData(tableConfig, metrics)

      // Cache if we need to do multiple operations
      sourceData.cache()

      // Validate schema compatibility
      if (!targetWriter.validateSchema(sourceData, tableConfig)) {
        throw new IllegalStateException("Schema validation failed")
      }

      // Write to target
      writeToTarget(sourceData, tableConfig, metrics)

      // Unpersist cached data
      sourceData.unpersist()

      metrics.recordsTransferred
    } match {
      case Success(rowCount) =>
        val duration = System.currentTimeMillis() - startTime
        val result = TransferResult(
          tableName = tableConfig.sourceTable,
          success = true,
          rowsTransferred = rowCount,
          durationMs = duration,
          errorMessage = None
        )
        auditLogger.foreach(_.logTransfer(result))
        logger.info(s"Transfer completed: ${tableConfig.sourceTable}, rows=$rowCount, duration=${duration}ms")
        result

      case Failure(ex) =>
        val duration = System.currentTimeMillis() - startTime
        val result = TransferResult(
          tableName = tableConfig.sourceTable,
          success = false,
          rowsTransferred = 0,
          durationMs = duration,
          errorMessage = Some(ex.getMessage)
        )
        auditLogger.foreach(_.logTransfer(result))
        logger.error(s"Transfer failed: ${tableConfig.sourceTable}", ex)
        result
    }
  }

  /**
   * Read data from source cluster via JDBC.
   */
  private def readSourceData(
    tableConfig: TableTransferConfig,
    metrics: TransferMetrics
  ): DataFrame = {
    val df = if (config.parallelism > 1 && hasNumericPartitionColumn(tableConfig)) {
      // Use partitioned reads for better parallelism
      val partitionCol = detectPartitionColumn(tableConfig)
      val bounds = getPartitionBounds(tableConfig, partitionCol)
      sourceReader.readTablePartitioned(
        tableConfig,
        partitionCol,
        bounds._1,
        bounds._2,
        config.parallelism
      )
    } else {
      sourceReader.readTable(tableConfig)
    }

    metrics.recordsRead = df.count()
    logger.info(s"Read ${metrics.recordsRead} rows from source")
    df
  }

  /**
   * Write data to target cluster.
   */
  private def writeToTarget(
    df: DataFrame,
    tableConfig: TableTransferConfig,
    metrics: TransferMetrics
  ): Unit = {
    targetWriter.writeTable(df, tableConfig)
    metrics.recordsTransferred = df.count()
  }

  /**
   * Check if table has a numeric column suitable for partitioned reads.
   */
  private def hasNumericPartitionColumn(tableConfig: TableTransferConfig): Boolean = {
    tableConfig.columns.exists(col =>
      col.toLowerCase.endsWith("_id") ||
      col.toLowerCase == "id" ||
      col.toLowerCase.contains("key")
    )
  }

  /**
   * Detect a suitable partition column for parallel reads.
   */
  private def detectPartitionColumn(tableConfig: TableTransferConfig): String = {
    tableConfig.columns.find(_.toLowerCase == "id")
      .orElse(tableConfig.columns.find(_.toLowerCase.endsWith("_id")))
      .orElse(tableConfig.columns.find(_.toLowerCase.contains("key")))
      .getOrElse(tableConfig.columns.head)
  }

  /**
   * Get min/max bounds for partition column.
   */
  private def getPartitionBounds(
    tableConfig: TableTransferConfig,
    partitionColumn: String
  ): (Long, Long) = {
    // Query bounds from source
    val boundsQuery = tableConfig.filterCondition match {
      case Some(filter) =>
        s"(SELECT MIN($partitionColumn) as min_val, MAX($partitionColumn) as max_val " +
        s"FROM ${config.sourceCluster.hiveDatabase}.${tableConfig.sourceTable} " +
        s"WHERE $filter) AS bounds_query"
      case None =>
        s"(SELECT MIN($partitionColumn) as min_val, MAX($partitionColumn) as max_val " +
        s"FROM ${config.sourceCluster.hiveDatabase}.${tableConfig.sourceTable}) AS bounds_query"
    }

    val bounds = spark.read
      .jdbc(config.sourceCluster.hiveJdbcUrl, boundsQuery, new java.util.Properties())
      .first()

    (bounds.getLong(0), bounds.getLong(1))
  }

  private def logSummary(results: Map[String, TransferResult]): Unit = {
    val successful = results.values.count(_.success)
    val failed = results.values.count(!_.success)
    val totalRows = results.values.map(_.rowsTransferred).sum
    val totalTime = results.values.map(_.durationMs).sum

    logger.info("=" * 60)
    logger.info(s"Transfer Job Summary: ${config.jobId}")
    logger.info(s"Successful transfers: $successful")
    logger.info(s"Failed transfers: $failed")
    logger.info(s"Total rows transferred: $totalRows")
    logger.info(s"Total duration: ${totalTime}ms")
    logger.info("=" * 60)

    if (failed > 0) {
      logger.warn("Failed tables:")
      results.filter(!_._2.success).foreach { case (table, result) =>
        logger.warn(s"  - $table: ${result.errorMessage.getOrElse("Unknown error")}")
      }
    }
  }
}

/**
 * Result of a single table transfer.
 */
case class TransferResult(
  tableName: String,
  success: Boolean,
  rowsTransferred: Long,
  durationMs: Long,
  errorMessage: Option[String]
)
