package dev.bigspark.metrics

import org.apache.spark.sql.DataFrame
import java.time.{Instant, ZoneOffset}
import org.apache.spark.sql.functions._

case class JobMetrics(
  jobName: String,
  startTime: Instant,
  endTime: Instant,
  inputRecordCount: Long,
  outputRecordCount: Long,
  executionTimeSeconds: Long,
  inputPartitionCount: Int,
  outputPartitionCount: Int
)

object MetricsCollector {
  def collectMetrics(jobName: String, startTime: Instant, inputDF: DataFrame, outputDF: DataFrame): JobMetrics = {
    val endTime = Instant.now()
    
    JobMetrics(
      jobName = jobName,
      startTime = startTime,
      endTime = endTime,
      inputRecordCount = inputDF.count(),
      outputRecordCount = outputDF.count(),
      executionTimeSeconds = java.time.Duration.between(startTime, endTime).getSeconds,
      inputPartitionCount = inputDF.rdd.getNumPartitions,
      outputPartitionCount = outputDF.rdd.getNumPartitions
    )
  }
  
  def saveMetrics(metrics: JobMetrics, spark: org.apache.spark.sql.SparkSession, metricsTable: String): Unit = {
    import spark.implicits._
    
    val metricsDF = Seq(metrics).toDF()
      .withColumn("collection_timestamp", lit(Instant.now().atOffset(ZoneOffset.UTC).toString))
    
    metricsDF.write
      .format("iceberg")
      .mode("append")
      .saveAsTable(metricsTable)
  }
} 