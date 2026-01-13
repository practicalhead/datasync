package com.datasync.utils

import com.datasync.transfer.TransferResult
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.sql.Timestamp
import java.time.Instant

/**
 * Audit logging for data transfers.
 *
 * Provides:
 * - Persistent audit trail in Hive table
 * - Transfer tracking for compliance
 * - Job history and metrics
 *
 * Suitable for regulated environments requiring
 * data movement audit capabilities.
 */
class AuditLogger(spark: SparkSession, jobId: String) {

  private val logger = LoggerFactory.getLogger(classOf[AuditLogger])
  private val auditDatabase = "datasync_audit"
  private val auditTable = "transfer_audit_log"

  private val auditSchema = StructType(Seq(
    StructField("job_id", StringType, nullable = false),
    StructField("table_name", StringType, nullable = false),
    StructField("transfer_timestamp", TimestampType, nullable = false),
    StructField("source_cluster", StringType, nullable = true),
    StructField("target_cluster", StringType, nullable = true),
    StructField("rows_transferred", LongType, nullable = false),
    StructField("duration_ms", LongType, nullable = false),
    StructField("success", BooleanType, nullable = false),
    StructField("error_message", StringType, nullable = true),
    StructField("executed_by", StringType, nullable = true),
    StructField("filter_applied", StringType, nullable = true),
    StructField("columns_selected", StringType, nullable = true)
  ))

  // Initialize audit table on construction
  initAuditTable()

  /**
   * Log a transfer result to the audit table.
   */
  def logTransfer(
    result: TransferResult,
    sourceCluster: String = "",
    targetCluster: String = "",
    filterApplied: Option[String] = None,
    columnsSelected: Seq[String] = Seq.empty
  ): Unit = {
    try {
      val auditRow = Row(
        jobId,
        result.tableName,
        Timestamp.from(Instant.now()),
        sourceCluster,
        targetCluster,
        result.rowsTransferred,
        result.durationMs,
        result.success,
        result.errorMessage.orNull,
        System.getProperty("user.name"),
        filterApplied.orNull,
        if (columnsSelected.nonEmpty) columnsSelected.mkString(",") else null
      )

      val auditDf = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(auditRow)),
        auditSchema
      )

      auditDf.write
        .mode("append")
        .insertInto(s"$auditDatabase.$auditTable")

      logger.info(s"Audit logged: ${result.tableName}, success=${result.success}")
    } catch {
      case ex: Exception =>
        logger.warn(s"Failed to write audit log: ${ex.getMessage}")
    }
  }

  /**
   * Log start of a transfer job.
   */
  def logJobStart(tables: Seq[String]): Unit = {
    logger.info(s"Transfer job started: $jobId")
    logger.info(s"Tables: ${tables.mkString(", ")}")
  }

  /**
   * Log completion of a transfer job.
   */
  def logJobComplete(
    successCount: Int,
    failureCount: Int,
    totalRows: Long,
    totalDuration: Long
  ): Unit = {
    logger.info(s"Transfer job completed: $jobId")
    logger.info(s"Success: $successCount, Failures: $failureCount")
    logger.info(s"Total rows: $totalRows, Duration: ${totalDuration}ms")
  }

  /**
   * Query audit history for a table.
   */
  def getTableHistory(tableName: String, limit: Int = 100): org.apache.spark.sql.DataFrame = {
    spark.sql(
      s"""
         |SELECT *
         |FROM $auditDatabase.$auditTable
         |WHERE table_name = '$tableName'
         |ORDER BY transfer_timestamp DESC
         |LIMIT $limit
         |""".stripMargin)
  }

  /**
   * Query audit history for a job.
   */
  def getJobHistory(targetJobId: String): org.apache.spark.sql.DataFrame = {
    spark.sql(
      s"""
         |SELECT *
         |FROM $auditDatabase.$auditTable
         |WHERE job_id = '$targetJobId'
         |ORDER BY transfer_timestamp DESC
         |""".stripMargin)
  }

  /**
   * Get transfer statistics summary.
   */
  def getTransferStats(days: Int = 30): org.apache.spark.sql.DataFrame = {
    spark.sql(
      s"""
         |SELECT
         |  table_name,
         |  COUNT(*) as transfer_count,
         |  SUM(rows_transferred) as total_rows,
         |  AVG(duration_ms) as avg_duration_ms,
         |  SUM(CASE WHEN success THEN 1 ELSE 0 END) as success_count,
         |  SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) as failure_count
         |FROM $auditDatabase.$auditTable
         |WHERE transfer_timestamp >= date_sub(current_date(), $days)
         |GROUP BY table_name
         |ORDER BY transfer_count DESC
         |""".stripMargin)
  }

  private def initAuditTable(): Unit = {
    try {
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $auditDatabase")
      spark.sql(
        s"""
           |CREATE TABLE IF NOT EXISTS $auditDatabase.$auditTable (
           |  job_id STRING,
           |  table_name STRING,
           |  transfer_timestamp TIMESTAMP,
           |  source_cluster STRING,
           |  target_cluster STRING,
           |  rows_transferred BIGINT,
           |  duration_ms BIGINT,
           |  success BOOLEAN,
           |  error_message STRING,
           |  executed_by STRING,
           |  filter_applied STRING,
           |  columns_selected STRING
           |)
           |STORED AS PARQUET
           |""".stripMargin)
      logger.info("Audit table initialized")
    } catch {
      case ex: Exception =>
        logger.warn(s"Could not initialize audit table: ${ex.getMessage}")
    }
  }
}
