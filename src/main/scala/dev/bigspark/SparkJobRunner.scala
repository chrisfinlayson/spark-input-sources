package dev.bigspark

import org.apache.spark.sql.DataFrame
import dev.bigspark.datasources.InputSources
import dev.bigspark.metrics.{MetricsCollector, JobMetrics}
import com.typesafe.config.{ConfigFactory, Config}
import pureconfig._
import pureconfig.generic.auto._
import scala.util.{Try, Success, Failure}
import java.time.Instant

object SparkJobRunner extends SparkSessionWrapper {
  
  case class IcebergOutputConfig(
    catalogName: String,
    databaseName: String,
    tableName: String,
    partitionBy: Option[Seq[String]] = None,
    properties: Option[Map[String, String]] = None
  ) {
    def fullTableName: String = s"$catalogName.$databaseName.$tableName"
  }
  
  case class JobConfig(
    jobName: String,
    input: InputSources,
    output: IcebergOutputConfig,
    metricsTable: String,
    writeMode: String = "overwrite"
  )

  def validateConfig(config: JobConfig): Either[String, JobConfig] = {
    for {
      _ <- validateJobName(config.jobName)
      _ <- validateIcebergConfig(config.output)
      _ <- validateMetricsTable(config.metricsTable)
    } yield config
  }
  
  private def validateJobName(name: String): Either[String, String] = {
    if (name.isEmpty) Left("Job name cannot be empty")
    else if (name.length > 100) Left("Job name too long")
    else Right(name)
  }
  
  private def validateIcebergConfig(config: IcebergOutputConfig): Either[String, IcebergOutputConfig] = {
    if (config.catalogName.isEmpty) Left("Catalog name cannot be empty")
    else if (config.databaseName.isEmpty) Left("Database name cannot be empty")
    else if (config.tableName.isEmpty) Left("Table name cannot be empty")
    else Right(config)
  }
  
  private def validateMetricsTable(table: String): Either[String, String] = {
    if (table.isEmpty) Left("Metrics table cannot be empty")
    else if (!table.contains(".")) Left("Metrics table must be fully qualified")
    else Right(table)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: spark-submit ... SparkJobRunner <config_file_path>")
      System.exit(1)
    }

    val configPath = args(0)
    val startTime = Instant.now()
    
    Try {
      // Load and parse HOCON configuration
      val config = ConfigSource.file(configPath).loadOrThrow[JobConfig]
      
      // Validate configuration
      validateConfig(config) match {
        case Left(error) => 
          throw new IllegalArgumentException(s"Configuration validation failed: $error")
        case Right(validConfig) =>
          // Load the input data
          val inputDF = validConfig.input.loadData
          
          // Configure Iceberg write options
          val writer = spark.write
            .format("iceberg")
            .mode(validConfig.writeMode)
          
          // Add optional properties
          validConfig.output.properties.foreach(_.foreach { case (key, value) =>
            writer.option(key, value)
          })
          
          // Add partitioning if specified
          val finalWriter = validConfig.output.partitionBy match {
            case Some(columns) => writer.partitionBy(columns: _*)
            case None => writer
          }
          
          // Write the output
          finalWriter.saveAsTable(validConfig.output.fullTableName)
          
          // Get output DataFrame for metrics
          val outputDF = spark.table(validConfig.output.fullTableName)
          
          // Collect and save metrics
          val metrics = MetricsCollector.collectMetrics(
            validConfig.jobName,
            startTime,
            inputDF,
            outputDF
          )
          
          MetricsCollector.saveMetrics(metrics, spark, validConfig.metricsTable)
          
          println(s"""
            |Job completed successfully:
            |Job Name: ${metrics.jobName}
            |Execution Time: ${metrics.executionTimeSeconds}s
            |Input Records: ${metrics.inputRecordCount}
            |Output Records: ${metrics.outputRecordCount}
            |""".stripMargin)
          
    } match {
      case Success(_) => 
        spark.stop()
      case Failure(e) =>
        println(s"Job failed with error: ${e.getMessage}")
        e.printStackTrace()
        spark.stop()
        System.exit(1)
    }
  }
} 