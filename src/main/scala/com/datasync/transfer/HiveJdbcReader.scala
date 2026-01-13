package com.datasync.transfer

import com.datasync.config.{ClusterConfig, TableTransferConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.util.Properties

/**
 * Reads data from source Hive cluster via JDBC connection to HiveServer2.
 *
 * This approach allows Spark running on the target cluster to read from
 * the source cluster without requiring:
 * - Shared storage/HDFS between clusters
 * - Multiple SparkContexts
 * - distcp file transfer
 *
 * The data flows directly through Spark executors via JDBC, enabling:
 * - Row-level filtering (pushed down to source when possible)
 * - Column selection (only selected columns transferred)
 * - Parallel reading using partition predicates
 */
class HiveJdbcReader(spark: SparkSession, clusterConfig: ClusterConfig) {

  private val logger = LoggerFactory.getLogger(classOf[HiveJdbcReader])

  private val connectionProperties: Properties = {
    val props = new Properties()
    props.setProperty("driver", "org.apache.hive.jdbc.HiveDriver")

    if (clusterConfig.kerberosEnabled) {
      props.setProperty("AuthMech", "1")
      props.setProperty("KrbRealm", extractKerberosRealm(clusterConfig.kerberosPrincipal.get))
      props.setProperty("KrbHostFQDN", extractKerberosHost(clusterConfig.hiveJdbcUrl))
      props.setProperty("KrbServiceName", "hive")
    }
    props
  }

  /**
   * Read table data with filtering and column selection.
   *
   * @param tableConfig Configuration specifying source table, columns, and filter
   * @return DataFrame with selected columns and filtered rows
   */
  def readTable(tableConfig: TableTransferConfig): DataFrame = {
    val fullTableName = s"${clusterConfig.hiveDatabase}.${tableConfig.sourceTable}"

    // Build column selection
    val columnList = if (tableConfig.columns.isEmpty || tableConfig.columns.contains("*")) {
      "*"
    } else {
      tableConfig.columns.mkString(", ")
    }

    // Build query with filter pushdown
    val query = tableConfig.filterCondition match {
      case Some(filter) =>
        s"(SELECT $columnList FROM $fullTableName WHERE $filter) AS source_data"
      case None =>
        s"(SELECT $columnList FROM $fullTableName) AS source_data"
    }

    logger.info(s"Reading from source cluster via JDBC: $query")
    logger.info(s"JDBC URL: ${clusterConfig.hiveJdbcUrl}")

    spark.read
      .jdbc(clusterConfig.hiveJdbcUrl, query, connectionProperties)
  }

  /**
   * Read table with parallel partitioned reads for better performance.
   *
   * @param tableConfig Configuration for the table transfer
   * @param partitionColumn Column to use for partitioning reads
   * @param lowerBound Lower bound of partition column
   * @param upperBound Upper bound of partition column
   * @param numPartitions Number of parallel partitions to read
   * @return DataFrame with partitioned parallel reads
   */
  def readTablePartitioned(
    tableConfig: TableTransferConfig,
    partitionColumn: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int
  ): DataFrame = {
    val fullTableName = s"${clusterConfig.hiveDatabase}.${tableConfig.sourceTable}"

    val columnList = if (tableConfig.columns.isEmpty || tableConfig.columns.contains("*")) {
      "*"
    } else {
      tableConfig.columns.mkString(", ")
    }

    val query = tableConfig.filterCondition match {
      case Some(filter) =>
        s"(SELECT $columnList FROM $fullTableName WHERE $filter) AS source_data"
      case None =>
        s"(SELECT $columnList FROM $fullTableName) AS source_data"
    }

    logger.info(s"Reading with partitioned JDBC: $query")
    logger.info(s"Partition column: $partitionColumn, bounds: [$lowerBound, $upperBound], partitions: $numPartitions")

    spark.read
      .jdbc(
        clusterConfig.hiveJdbcUrl,
        query,
        partitionColumn,
        lowerBound,
        upperBound,
        numPartitions,
        connectionProperties
      )
  }

  /**
   * Read table using custom partition predicates for non-numeric columns.
   *
   * @param tableConfig Configuration for the table transfer
   * @param predicates Array of WHERE clause predicates for each partition
   * @return DataFrame with custom partitioned reads
   */
  def readTableWithPredicates(
    tableConfig: TableTransferConfig,
    predicates: Array[String]
  ): DataFrame = {
    val fullTableName = s"${clusterConfig.hiveDatabase}.${tableConfig.sourceTable}"

    val columnList = if (tableConfig.columns.isEmpty || tableConfig.columns.contains("*")) {
      "*"
    } else {
      tableConfig.columns.mkString(", ")
    }

    // Combine base filter with partition predicates
    val baseQuery = tableConfig.filterCondition match {
      case Some(filter) =>
        s"(SELECT $columnList FROM $fullTableName WHERE $filter) AS source_data"
      case None =>
        s"(SELECT $columnList FROM $fullTableName) AS source_data"
    }

    logger.info(s"Reading with predicate partitions: $baseQuery")
    logger.info(s"Number of partition predicates: ${predicates.length}")

    spark.read
      .jdbc(clusterConfig.hiveJdbcUrl, baseQuery, predicates, connectionProperties)
  }

  /**
   * Get row count for a table with optional filter (useful for planning).
   */
  def getRowCount(tableConfig: TableTransferConfig): Long = {
    val fullTableName = s"${clusterConfig.hiveDatabase}.${tableConfig.sourceTable}"

    val countQuery = tableConfig.filterCondition match {
      case Some(filter) =>
        s"(SELECT COUNT(*) as cnt FROM $fullTableName WHERE $filter) AS count_query"
      case None =>
        s"(SELECT COUNT(*) as cnt FROM $fullTableName) AS count_query"
    }

    spark.read
      .jdbc(clusterConfig.hiveJdbcUrl, countQuery, connectionProperties)
      .first()
      .getLong(0)
  }

  private def extractKerberosRealm(principal: String): String = {
    principal.split("@").lastOption.getOrElse("")
  }

  private def extractKerberosHost(jdbcUrl: String): String = {
    // Extract host from jdbc:hive2://host:port/...
    val pattern = "jdbc:hive2://([^:/]+)".r
    pattern.findFirstMatchIn(jdbcUrl).map(_.group(1)).getOrElse("")
  }
}
