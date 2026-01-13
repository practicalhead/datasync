package com.datasync.utils

/**
 * Tracks metrics for a table transfer operation.
 */
case class TransferMetrics(
  tableName: String,
  var recordsRead: Long = 0,
  var recordsTransferred: Long = 0,
  var bytesTransferred: Long = 0,
  var startTime: Long = System.currentTimeMillis(),
  var endTime: Long = 0
) {

  def duration: Long = {
    if (endTime > 0) endTime - startTime
    else System.currentTimeMillis() - startTime
  }

  def recordsPerSecond: Double = {
    val dur = duration / 1000.0
    if (dur > 0) recordsTransferred / dur else 0
  }

  def complete(): Unit = {
    endTime = System.currentTimeMillis()
  }

  override def toString: String = {
    s"TransferMetrics(table=$tableName, read=$recordsRead, transferred=$recordsTransferred, " +
    s"duration=${duration}ms, rate=${recordsPerSecond.formatted("%.2f")} rec/s)"
  }
}

/**
 * Aggregates metrics across multiple table transfers.
 */
class JobMetrics(jobId: String) {

  private var tableMetrics: Map[String, TransferMetrics] = Map.empty
  private val jobStartTime: Long = System.currentTimeMillis()
  private var jobEndTime: Long = 0

  def addTableMetrics(metrics: TransferMetrics): Unit = {
    tableMetrics = tableMetrics + (metrics.tableName -> metrics)
  }

  def getTableMetrics(tableName: String): Option[TransferMetrics] = {
    tableMetrics.get(tableName)
  }

  def complete(): Unit = {
    jobEndTime = System.currentTimeMillis()
  }

  def totalRecordsRead: Long = tableMetrics.values.map(_.recordsRead).sum
  def totalRecordsTransferred: Long = tableMetrics.values.map(_.recordsTransferred).sum
  def totalDuration: Long = if (jobEndTime > 0) jobEndTime - jobStartTime else System.currentTimeMillis() - jobStartTime
  def tablesProcessed: Int = tableMetrics.size

  def summary: String = {
    s"""
       |Job Metrics Summary: $jobId
       |  Tables processed: $tablesProcessed
       |  Total records read: $totalRecordsRead
       |  Total records transferred: $totalRecordsTransferred
       |  Total duration: ${totalDuration}ms
       |  Overall rate: ${if (totalDuration > 0) totalRecordsTransferred / (totalDuration / 1000.0) else 0} rec/s
       |""".stripMargin
  }
}
