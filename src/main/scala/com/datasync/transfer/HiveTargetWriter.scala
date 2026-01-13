package com.datasync.transfer

import com.datasync.config.{ClusterConfig, TableTransferConfig}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Writes data to target Hive cluster using native Spark-Hive integration.
 *
 * Since Spark runs on the target cluster, it has native access to:
 * - Target Hive metastore
 * - Target HDFS storage
 *
 * This enables:
 * - Proper Hive table creation and metadata management
 * - Efficient writes to target HDFS
 * - Support for partitioned tables
 * - Various file formats (Parquet, ORC, etc.)
 */
class HiveTargetWriter(spark: SparkSession, clusterConfig: ClusterConfig) {

  private val logger = LoggerFactory.getLogger(classOf[HiveTargetWriter])

  /**
   * Write DataFrame to target Hive table.
   *
   * @param df DataFrame to write
   * @param tableConfig Configuration specifying target table and write mode
   * @param format Storage format (default: parquet)
   */
  def writeTable(
    df: DataFrame,
    tableConfig: TableTransferConfig,
    format: String = "parquet"
  ): Unit = {
    val fullTableName = s"${clusterConfig.hiveDatabase}.${tableConfig.targetTable}"
    val saveMode = parseSaveMode(tableConfig.writeMode)

    logger.info(s"Writing to target table: $fullTableName")
    logger.info(s"Write mode: ${tableConfig.writeMode}, Format: $format")
    logger.info(s"Row count: ${df.count()}")

    // Ensure database exists
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${clusterConfig.hiveDatabase}")

    if (tableConfig.partitionColumns.nonEmpty) {
      writePartitionedTable(df, fullTableName, tableConfig.partitionColumns, saveMode, format)
    } else {
      writeNonPartitionedTable(df, fullTableName, saveMode, format)
    }

    logger.info(s"Successfully wrote data to $fullTableName")
  }

  /**
   * Write to a partitioned Hive table.
   */
  private def writePartitionedTable(
    df: DataFrame,
    tableName: String,
    partitionColumns: Seq[String],
    saveMode: SaveMode,
    format: String
  ): Unit = {
    logger.info(s"Writing partitioned table with columns: ${partitionColumns.mkString(", ")}")

    df.write
      .mode(saveMode)
      .format(format)
      .partitionBy(partitionColumns: _*)
      .option("compression", "snappy")
      .saveAsTable(tableName)

    // Recover partitions to ensure metastore is updated
    spark.sql(s"MSCK REPAIR TABLE $tableName")
  }

  /**
   * Write to a non-partitioned Hive table.
   */
  private def writeNonPartitionedTable(
    df: DataFrame,
    tableName: String,
    saveMode: SaveMode,
    format: String
  ): Unit = {
    df.write
      .mode(saveMode)
      .format(format)
      .option("compression", "snappy")
      .saveAsTable(tableName)
  }

  /**
   * Write using insertInto for existing tables (schema must match).
   */
  def insertIntoTable(
    df: DataFrame,
    tableConfig: TableTransferConfig,
    overwrite: Boolean = false
  ): Unit = {
    val fullTableName = s"${clusterConfig.hiveDatabase}.${tableConfig.targetTable}"

    logger.info(s"Inserting into existing table: $fullTableName (overwrite=$overwrite)")

    if (overwrite) {
      df.write.mode(SaveMode.Overwrite).insertInto(fullTableName)
    } else {
      df.write.mode(SaveMode.Append).insertInto(fullTableName)
    }
  }

  /**
   * Create target table from source schema if it doesn't exist.
   */
  def createTableIfNotExists(
    df: DataFrame,
    tableConfig: TableTransferConfig,
    format: String = "parquet"
  ): Unit = {
    val fullTableName = s"${clusterConfig.hiveDatabase}.${tableConfig.targetTable}"

    if (!tableExists(tableConfig.targetTable)) {
      logger.info(s"Creating table schema for: $fullTableName")

      // Create empty table with schema
      val emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], df.schema)

      if (tableConfig.partitionColumns.nonEmpty) {
        emptyDf.write
          .format(format)
          .partitionBy(tableConfig.partitionColumns: _*)
          .saveAsTable(fullTableName)
      } else {
        emptyDf.write
          .format(format)
          .saveAsTable(fullTableName)
      }
    }
  }

  /**
   * Check if table exists in target database.
   */
  def tableExists(tableName: String): Boolean = {
    spark.catalog.tableExists(clusterConfig.hiveDatabase, tableName)
  }

  /**
   * Get table schema from target.
   */
  def getTableSchema(tableName: String): org.apache.spark.sql.types.StructType = {
    spark.table(s"${clusterConfig.hiveDatabase}.$tableName").schema
  }

  /**
   * Validate that source DataFrame schema is compatible with target table.
   */
  def validateSchema(df: DataFrame, tableConfig: TableTransferConfig): Boolean = {
    if (!tableExists(tableConfig.targetTable)) {
      return true // No target table, schema will be created from source
    }

    val targetSchema = getTableSchema(tableConfig.targetTable)
    val sourceColumns = df.schema.fieldNames.toSet
    val targetColumns = targetSchema.fieldNames.toSet

    // Check if all source columns exist in target
    val missingInTarget = sourceColumns -- targetColumns
    if (missingInTarget.nonEmpty) {
      logger.warn(s"Columns in source but not in target: ${missingInTarget.mkString(", ")}")
      return false
    }

    true
  }

  private def parseSaveMode(mode: String): SaveMode = {
    mode.toLowerCase match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "ignore" => SaveMode.Ignore
      case "error" | "errorifexists" => SaveMode.ErrorIfExists
      case _ => SaveMode.Overwrite
    }
  }
}
