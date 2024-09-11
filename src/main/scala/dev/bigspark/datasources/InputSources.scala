package dev.bigspark.datasources

import dev.bigspark.SparkSessionWrapper

import org.apache.spark.sql.DataFrame

/**
 * With this sealed trait, we can use InputSources for loading data from
 * Spark file(s), query, or a table.
 */
sealed trait InputSources {

  def loadData: DataFrame
}

object InputSources {
  final case class FileSource(
                               filePath: String,
                               filter: Option[String] = None,
                               format: String,
                               versionOrTime: Option[String] = None,
                               optionValue: Option[String] = None
                             ) extends InputSources with SparkSessionWrapper {

    override def loadData: DataFrame = {

      if (filter.isDefined) {
        withFilter
      }
      else {
        withoutFilter
      }
    }

    private def withFilter: DataFrame = {
      if (format != "delta" & versionOrTime.isEmpty & optionValue.isEmpty) {
        spark.read.format(format).load(filePath).filter(filter.get)
      }
      else {
        exceptionCheck()
        spark.read.format(format)
          .option(optionValue.get, versionOrTime.get)
          .load(filePath)
          .filter(filter.get)
      }
    }

    private def withoutFilter: DataFrame = {
      if (format != "delta" & versionOrTime.isEmpty & optionValue.isEmpty) {
        spark.read.format(format).load(filePath)
      }
      else {
        exceptionCheck()
        spark.read.format(format)
          .option(optionValue.get, versionOrTime.get)
          .load(filePath)
      }
    }

    private def exceptionCheck(): Unit = {
      if (versionOrTime.isDefined & format != "delta") {
        throw new IllegalArgumentException("versionOrTime cannot be defined when fileType is not delta.")
      }
      if (optionValue.isDefined & format != "delta") {
        throw new IllegalArgumentException("optionValue cannot be defined when fileType is not delta.")
      }
      if (optionValue.isDefined & versionOrTime.isEmpty) {
        throw new IllegalArgumentException("optionValue cannot be defined when versionOrTime is empty.")
      }
      if (optionValue.isEmpty & versionOrTime.isDefined) {
        throw new IllegalArgumentException("versionOrTime cannot be defined when optionValue is empty.")
      }
    }
  }

  final case class QuerySource(query: String) extends
    InputSources with SparkSessionWrapper {

    override def loadData: DataFrame = {

      spark.sql(query)
    }
  }

  final case class TableSource(
                                tableName: String,
                                filter: Option[String] = None
                              ) extends InputSources with SparkSessionWrapper {

    override def loadData: DataFrame = {

      if (filter.isDefined) {
        spark.table(tableName).filter(filter.get)
      }
      else {
        spark.table(tableName)
      }
    }
  }

  final case class BigQuerySource(
                                   query: String,
                                   dataset: String,
                                   projectId: String,
                                   fasterExecution: Boolean = false
                                 ) extends InputSources with SparkSessionWrapper {

    override def loadData: DataFrame = {
      spark.conf.set("materializationDataset", dataset)
      spark.conf.set("viewsEnabled", "true")
      spark.conf.set("parentProject", projectId)
      
      if (fasterExecution) {
        // faster but creates temporary tables in the BQ account
        spark.read.format("bigquery")
          .option("project", projectId)
          .option("query", query)
          .load()
      }
      else {
        // slower but no table creation
        spark.read.format("bigquery")
          .option("project", projectId)
          .load(query)
      }
    }
  }

  final case class PostgresSource(
                                   url: String,
                                   table: String,
                                   user: String,
                                   password: String,
                                   filter: Option[String] = None
                                 ) extends InputSources with SparkSessionWrapper {

    override def loadData: DataFrame = {
      val baseDF = spark.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .load()

      filter match {
        case Some(filterCondition) => baseDF.filter(filterCondition)
        case None => baseDF
      }
    }
  }

  final case class IcebergSource(
                                  tableName: String,
                                  path: String,
                                  filter: Option[String] = None,
                                  snapshotId: Option[Long] = None
                                ) extends InputSources with SparkSessionWrapper {

    override def loadData: DataFrame = {
      val baseDF = snapshotId match {
        case Some(id) => spark.read.format("iceberg").option("snapshot-id", id).option("path", path).table(tableName)
        case None => spark.read.format("iceberg").option("path", path).table(tableName)
      }

      filter match {
        case Some(filterCondition) => baseDF.filter(filterCondition)
        case None => baseDF
      }
    }
  }
}
