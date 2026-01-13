package com.datasync.config

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters._

/**
 * Configuration for cross-cluster Hive data transfer.
 *
 * Supports secure data transfer between Cloudera clusters using:
 * - JDBC connection to source HiveServer2
 * - Native Spark-Hive integration for target cluster
 */
case class ClusterConfig(
  name: String,
  hiveJdbcUrl: String,
  hiveDatabase: String,
  kerberosEnabled: Boolean,
  kerberosPrincipal: Option[String],
  kerberosKeytab: Option[String]
)

case class TableTransferConfig(
  sourceTable: String,
  targetTable: String,
  columns: Seq[String],
  filterCondition: Option[String],
  partitionColumns: Seq[String],
  writeMode: String  // overwrite, append, ignore, error
)

case class TransferJobConfig(
  jobId: String,
  sourceCluster: ClusterConfig,
  targetCluster: ClusterConfig,
  tables: Seq[TableTransferConfig],
  batchSize: Int,
  parallelism: Int,
  enableAudit: Boolean
)

object TransferConfig {

  def fromConfig(config: Config): TransferJobConfig = {
    val sourceConfig = config.getConfig("source")
    val targetConfig = config.getConfig("target")
    val tablesConfig = config.getConfigList("tables").asScala

    TransferJobConfig(
      jobId = config.getString("job.id"),
      sourceCluster = parseClusterConfig(sourceConfig, "source"),
      targetCluster = parseClusterConfig(targetConfig, "target"),
      tables = tablesConfig.map(parseTableConfig).toSeq,
      batchSize = config.getInt("transfer.batchSize"),
      parallelism = config.getInt("transfer.parallelism"),
      enableAudit = config.getBoolean("transfer.enableAudit")
    )
  }

  def fromFile(path: String): TransferJobConfig = {
    val config = ConfigFactory.parseFile(new java.io.File(path))
    fromConfig(config)
  }

  private def parseClusterConfig(config: Config, name: String): ClusterConfig = {
    ClusterConfig(
      name = name,
      hiveJdbcUrl = config.getString("hive.jdbcUrl"),
      hiveDatabase = config.getString("hive.database"),
      kerberosEnabled = config.getBoolean("kerberos.enabled"),
      kerberosPrincipal = if (config.getBoolean("kerberos.enabled"))
        Some(config.getString("kerberos.principal")) else None,
      kerberosKeytab = if (config.getBoolean("kerberos.enabled"))
        Some(config.getString("kerberos.keytab")) else None
    )
  }

  private def parseTableConfig(config: Config): TableTransferConfig = {
    TableTransferConfig(
      sourceTable = config.getString("source"),
      targetTable = config.getString("target"),
      columns = config.getStringList("columns").asScala.toSeq,
      filterCondition = if (config.hasPath("filter"))
        Some(config.getString("filter")) else None,
      partitionColumns = if (config.hasPath("partitionBy"))
        config.getStringList("partitionBy").asScala.toSeq else Seq.empty,
      writeMode = if (config.hasPath("writeMode"))
        config.getString("writeMode") else "overwrite"
    )
  }
}
