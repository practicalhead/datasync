package com.datasync

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.util.Properties

/**
 * Simplified single-table transfer for quick ad-hoc transfers.
 *
 * This provides a simpler interface when you just need to transfer
 * one table with filtering and column selection without full
 * configuration file setup.
 *
 * Usage:
 *   spark-submit \
 *     --class com.datasync.SimpleTransfer \
 *     --master yarn \
 *     hive-cross-cluster-transfer-1.0.0.jar \
 *     --source-jdbc "jdbc:hive2://source-host:10000/default" \
 *     --source-table "source_db.my_table" \
 *     --target-table "target_db.my_table" \
 *     --columns "col1,col2,col3" \
 *     --filter "date_col >= '2024-01-01'"
 */
object SimpleTransfer {

  private val logger = LoggerFactory.getLogger(this.getClass)

  case class TransferArgs(
    sourceJdbc: String = "",
    sourceTable: String = "",
    targetTable: String = "",
    columns: String = "*",
    filter: Option[String] = None,
    partitionBy: Option[String] = None,
    writeMode: String = "overwrite",
    parallelism: Int = 10
  )

  def main(args: Array[String]): Unit = {
    val transferArgs = parseArgs(args)

    logger.info("Starting simple table transfer")
    logger.info(s"Source JDBC: ${transferArgs.sourceJdbc}")
    logger.info(s"Source table: ${transferArgs.sourceTable}")
    logger.info(s"Target table: ${transferArgs.targetTable}")
    logger.info(s"Columns: ${transferArgs.columns}")
    logger.info(s"Filter: ${transferArgs.filter.getOrElse("none")}")

    val spark = SparkSession.builder()
      .appName(s"SimpleTransfer-${transferArgs.sourceTable}")
      .enableHiveSupport()
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    try {
      transferTable(spark, transferArgs)
      logger.info("Transfer completed successfully")
    } finally {
      spark.stop()
    }
  }

  /**
   * Execute the table transfer.
   */
  def transferTable(spark: SparkSession, args: TransferArgs): Long = {
    // Build the source query
    val columnList = if (args.columns == "*") "*" else args.columns
    val query = args.filter match {
      case Some(f) => s"(SELECT $columnList FROM ${args.sourceTable} WHERE $f) AS src"
      case None => s"(SELECT $columnList FROM ${args.sourceTable}) AS src"
    }

    logger.info(s"Reading from source: $query")

    // Read from source via JDBC
    val connectionProps = new Properties()
    connectionProps.setProperty("driver", "org.apache.hive.jdbc.HiveDriver")

    val sourceData = spark.read
      .jdbc(args.sourceJdbc, query, connectionProps)

    // Cache for count
    sourceData.cache()
    val rowCount = sourceData.count()
    logger.info(s"Read $rowCount rows from source")

    // Determine save mode
    val saveMode = args.writeMode.toLowerCase match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "ignore" => SaveMode.Ignore
      case _ => SaveMode.Overwrite
    }

    // Write to target
    val writer = sourceData.write.mode(saveMode)

    // Apply partitioning if specified
    val finalWriter = args.partitionBy match {
      case Some(partCols) => writer.partitionBy(partCols.split(","): _*)
      case None => writer
    }

    logger.info(s"Writing to target: ${args.targetTable}")
    finalWriter.saveAsTable(args.targetTable)

    sourceData.unpersist()
    logger.info(s"Successfully wrote $rowCount rows to ${args.targetTable}")

    rowCount
  }

  private def parseArgs(args: Array[String]): TransferArgs = {
    var result = TransferArgs()

    args.sliding(2, 2).foreach {
      case Array("--source-jdbc", value) => result = result.copy(sourceJdbc = value)
      case Array("--source-table", value) => result = result.copy(sourceTable = value)
      case Array("--target-table", value) => result = result.copy(targetTable = value)
      case Array("--columns", value) => result = result.copy(columns = value)
      case Array("--filter", value) => result = result.copy(filter = Some(value))
      case Array("--partition-by", value) => result = result.copy(partitionBy = Some(value))
      case Array("--write-mode", value) => result = result.copy(writeMode = value)
      case Array("--parallelism", value) => result = result.copy(parallelism = value.toInt)
      case _ =>
    }

    require(result.sourceJdbc.nonEmpty, "--source-jdbc is required")
    require(result.sourceTable.nonEmpty, "--source-table is required")
    require(result.targetTable.nonEmpty, "--target-table is required")

    result
  }
}
