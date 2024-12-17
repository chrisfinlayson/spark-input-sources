package dev.bigspark.datasources

import dev.bigspark.SparkSessionWrapper
import dev.bigspark.security.VaultCredentialsManager

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
      val baseDF = if (format != "delta" && versionOrTime.isEmpty && optionValue.isEmpty) {
        spark.read.format(format).load(filePath)
      } else {
        exceptionCheck()
        spark.read.format(format)
          .option(optionValue.get, versionOrTime.get)
          .load(filePath)
      }

      filter match {
        case Some(filterCondition) => baseDF.filter(filterCondition)
        case None => baseDF
      }
    }

    def exceptionCheck(): Unit = {
      if (versionOrTime.isDefined && format != "delta") {
        throw new IllegalArgumentException("versionOrTime cannot be defined when fileType is not delta.")
      }
      if (optionValue.isDefined && format != "delta") {
        throw new IllegalArgumentException("optionValue cannot be defined when fileType is not delta.")
      }
      if (optionValue.isDefined && versionOrTime.isEmpty) {
        throw new IllegalArgumentException("optionValue cannot be defined when versionOrTime is empty.")
      }
      if (optionValue.isEmpty && versionOrTime.isDefined) {
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
      val baseDF = spark.table(tableName)
      filter match {
        case Some(filterCondition) => baseDF.filter(filterCondition)
        case None => baseDF
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
    credentials: Either[VaultCredentials, DirectCredentials] = Left(VaultCredentials(
      sys.env.getOrElse("VAULT_ADDR", "http://vault:8200"),
      sys.env.getOrElse("VAULT_TOKEN", "root"),
      sys.env.getOrElse("VAULT_PATH", "postgres")
    )),
    filter: Option[String] = None
  ) extends InputSources with SparkSessionWrapper {

    override def loadData: DataFrame = {
      val (username, password) = credentials match {
        case Left(vaultCreds) =>
          val vault = new VaultCredentialsManager(
            vaultCreds.vaultAddress,
            vaultCreds.vaultToken,
            vaultCreds.vaultPath
          )
          vault.getCredentials("postgres")
        case Right(directCreds) =>
          (directCreds.username, directCreds.password)
      }

      val baseDF = spark.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", username.toString)
        .option("password", password.toString)
        .option("driver", "org.postgresql.Driver")
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

  final case class JdbcSource(
    url: String,
    table: String,
    credentials: Either[VaultCredentials, DirectCredentials],
    filter: Option[String] = None,
    fetchSize: Option[Int] = None,
    partitionColumn: Option[String] = None,
    numPartitions: Option[Int] = None,
    lowerBound: Option[Long] = None,
    upperBound: Option[Long] = None
  ) extends InputSources with SparkSessionWrapper {

    def getDatabaseType: String = {
      url.toLowerCase match {
        case u if u.contains("postgresql") => "postgresql"
        case u if u.contains("oracle") => "oracle"
        case u if u.contains("sqlserver") => "sqlserver"
        case u if u.contains("teradata") => "teradata"
        case _ => throw new IllegalArgumentException(s"Unsupported database type in URL: $url")
      }
    }

    def getDriverClass: String = getDatabaseType match {
      case "postgresql" => "org.postgresql.Driver"
      case "oracle" => "oracle.jdbc.driver.OracleDriver"
      case "sqlserver" => "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      case "teradata" => "com.teradata.jdbc.TeraDriver"
    }

    override def loadData: DataFrame = {
      val (username, password) = credentials match {
        case Left(vaultCreds) =>
          val vault = new VaultCredentialsManager(
            vaultCreds.vaultAddress,
            vaultCreds.vaultToken,
            vaultCreds.vaultPath
          )
          vault.getCredentials(getDatabaseType)
        case Right(directCreds) =>
          (directCreds.username, directCreds.password)
      }

      val reader = spark.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", username.toString)
        .option("password", password.toString)
        .option("driver", getDriverClass)

      // Add optional configurations
      fetchSize.foreach(size => reader.option("fetchsize", size))
      
      // Add partitioning options if all required parameters are present
      if (partitionColumn.isDefined && numPartitions.isDefined && 
          lowerBound.isDefined && upperBound.isDefined) {
        reader
          .option("partitionColumn", partitionColumn.get)
          .option("numPartitions", numPartitions.get)
          .option("lowerBound", lowerBound.get)
          .option("upperBound", upperBound.get)
      }

      val baseDF = reader.load()

      filter match {
        case Some(filterCondition) => baseDF.filter(filterCondition)
        case None => baseDF
      }
    }
  }

  // Credential case classes
  case class VaultCredentials(
    vaultAddress: String,
    vaultToken: String,
    vaultPath: String
  )

  case class DirectCredentials(
    username: String,
    password: String
  )

  object JdbcSource {
    def fromConfig(config: com.typesafe.config.Config): JdbcSource = {
      JdbcSource(
        url = config.getString("url"),
        table = config.getString("table"),
        credentials = Right(DirectCredentials(
          username = config.getString("credentials.username"),
          password = config.getString("credentials.password")
        )),
        fetchSize = if (config.hasPath("fetch-size")) Some(config.getInt("fetch-size")) else None,
        partitionColumn = if (config.hasPath("partition-column")) Some(config.getString("partition-column")) else None,
        numPartitions = if (config.hasPath("num-partitions")) Some(config.getInt("num-partitions")) else None,
        lowerBound = if (config.hasPath("lower-bound")) Some(config.getLong("lower-bound")) else None,
        upperBound = if (config.hasPath("upper-bound")) Some(config.getLong("upper-bound")) else None,
        filter = if (config.hasPath("filter")) Some(config.getString("filter")) else None
      )
    }
  }
}
