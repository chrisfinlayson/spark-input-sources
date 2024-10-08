package dev.bigspark.datasources

import java.io.File

import scala.reflect.io.Directory

import dev.bigspark.HashDataFrame
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import pureconfig._
import pureconfig.generic.auto._

import org.apache.spark.sql.{DataFrame, SparkSession}


class InputSourcesSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  val saCredential: String = scala.util.Properties.envOrElse(
    "GOOGLE_GHA_CREDS_PATH",
    s"${System.getenv("HOME")}/dev/bigspark-reporting/secrets/client_secrets_bigquery.json"
  )

  val spark: SparkSession = SparkSession
    .builder()
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("credentialsFile", saCredential)
    .appName("InputSource Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // create temp table for testing
  spark.read.format("parquet")
    .load(getClass.getResource("/input_test_data").getPath)
    .createOrReplaceTempView("test_data")

  override def afterAll(): Unit = {

    new Directory(new File("spark-warehouse")).deleteRecursively
    spark.catalog.dropTempView("test_data")
    super.afterAll()
  }

  "TableSource" should {
    "return a df based on the table name and filter" in {
      val strConfig: String =
        """
        |{
        | type: table-source
        | table-name: test_data
        | filter: "x < 1"
        |}""".stripMargin
      val config: InputSources = ConfigSource.fromConfig(ConfigFactory.parseString(strConfig))
        .loadOrThrow[InputSources]
      val df: DataFrame = config.loadData.sort("y")
      val expectedDF: DataFrame = Seq(
        (0, 0),
        (0, 1),
        (0, 2),
        (0, 3)
      ).toDF("x", "y").sort("y")

      assert(
        HashDataFrame.checksumDataFrame(df, 1) == HashDataFrame.checksumDataFrame(expectedDF, 1)
      )
    }
  }

  "TableSource" should {
    "return a df based on the table name and without a filter" in {
      val strConfig: String =
        """
          |{
          | type: table-source
          | table-name: test_data
          |}""".stripMargin
      val config: InputSources = ConfigSource.fromConfig(ConfigFactory.parseString(strConfig))
        .loadOrThrow[InputSources]
      val df: DataFrame = config.loadData.sort("x", "y")
      val expectedDF: DataFrame = Seq(
        (0, 0),
        (0, 1),
        (0, 2),
        (0, 3),
        (1, 0),
        (1, 1),
        (1, 2),
        (1, 3),
        (3, 0),
        (3, 1),
        (3, 2),
        (3, 3),
        (2, 0),
        (2, 1),
        (2, 2),
        (2, 3)
      ).toDF("x", "y").sort("x", "y")

      assert(
        HashDataFrame.checksumDataFrame(df, 1) == HashDataFrame.checksumDataFrame(expectedDF, 1)
      )
    }
  }

  "QuerySource" should {
    "return a df based on the sql query" in {
      val strConfig: String =
        """
          |{
          | type: query-source
          | query: "SELECT * FROM test_data WHERE y = 1"
          |}""".stripMargin
      val config: InputSources = ConfigSource.fromConfig(ConfigFactory.parseString(strConfig))
        .loadOrThrow[InputSources]
      val df: DataFrame = config.loadData.sort("x")
      val expectedDF: DataFrame = Seq(
        (0, 1),
        (1, 1),
        (2, 1),
        (3, 1),
      ).toDF("x", "y").sort("x")

      assert(
        HashDataFrame.checksumDataFrame(df, 1) == HashDataFrame.checksumDataFrame(expectedDF, 1)
      )
    }
  }

  "FileSource" should {
    "return df based on the file path and filter" in {
      val strConfig: String =
        s"""
          |{
          | type: file-source
          | file-path: ${getClass.getResource("/input_test_data").getPath}
          | filter: "x = 2 AND y <= 2"
          | format: parquet
          |}""".stripMargin
      val config: InputSources = ConfigSource.fromConfig(ConfigFactory.parseString(strConfig))
        .loadOrThrow[InputSources]
      val df: DataFrame = config.loadData.sort("y")
      val expectedDF: DataFrame = Seq(
        (2, 0),
        (2, 1),
        (2, 2)
      ).toDF("x", "y").sort("y")

      assert(
        HashDataFrame.checksumDataFrame(df, 1) == HashDataFrame.checksumDataFrame(expectedDF, 1)
      )
    }
  }

  "FileSource" should {
    "return df based on the file path and without filter" in {
      val strConfig: String =
        s"""
           |{
           | type: file-source
           | file-path: ${getClass.getResource("/input_test_data").getPath}
           | format: parquet
           |}""".stripMargin
      val config: InputSources = ConfigSource.fromConfig(ConfigFactory.parseString(strConfig))
        .loadOrThrow[InputSources]
      val df: DataFrame = config.loadData.sort("x", "y")
      val expectedDF: DataFrame = Seq(
        (0, 0),
        (0, 1),
        (0, 2),
        (0, 3),
        (1, 0),
        (1, 1),
        (1, 2),
        (1, 3),
        (3, 0),
        (3, 1),
        (3, 2),
        (3, 3),
        (2, 0),
        (2, 1),
        (2, 2),
        (2, 3)
      ).toDF("x", "y").sort("x", "y")

      assert(
        HashDataFrame.checksumDataFrame(df, 1) == HashDataFrame.checksumDataFrame(expectedDF, 1)
      )
    }
  }

  "FileSource exceptionCheck" should {
    "throw IllegalArgumentException when delta is not specified but we have versionOrTime" in {
      val strConfig: String =
        s"""
           |{
           | type: file-source
           | file-path: ${getClass.getResource("/input_test_data").getPath}
           | filter: "x = 2 AND y <= 2"
           | format: parquet
           | version-or-time: 2
           |}""".stripMargin
      val config: InputSources = ConfigSource.fromConfig(ConfigFactory.parseString(strConfig))
       .loadOrThrow[InputSources]
      val thrown: IllegalArgumentException = the[IllegalArgumentException] thrownBy config.loadData

      assert(thrown.getMessage == "versionOrTime cannot be defined when fileType is not delta.")
    }
  }

  "FileSource exceptionCheck" should {
    "throw IllegalArgumentException when delta is not specified but we have optionValue" in {
      val strConfig: String =
        s"""
           |{
           | type: file-source
           | file-path: ${getClass.getResource("/input_test_data").getPath}
           | filter: "x = 2 AND y <= 2"
           | format: parquet
           | option-value: versionAsOf
           |}""".stripMargin
      val config: InputSources = ConfigSource.fromConfig(ConfigFactory.parseString(strConfig))
        .loadOrThrow[InputSources]
      val thrown: IllegalArgumentException = the[IllegalArgumentException] thrownBy config.loadData

      assert(thrown.getMessage == "optionValue cannot be defined when fileType is not delta.")
    }
  }

  "FileSource" should {
    "read the version 0 of the delta table" in {
      val strConfig: String =
        s"""
           |{
           | type: file-source
           | file-path: ${getClass.getResource("/input_test_data_delta").getPath}
           | filter: "x = 2 AND y <= 2"
           | format: delta
           | option-value: versionAsOf
           | version-or-time: 0
           |}""".stripMargin
      val config: InputSources = ConfigSource.fromConfig(ConfigFactory.parseString(strConfig))
        .loadOrThrow[InputSources]
      val df: DataFrame = config.loadData.sort("y")
      val expectedDF: DataFrame = Seq(
        (2, 0),
        (2, 1),
        (2, 2)
      ).toDF("x", "y").sort("y")

      assert(
        HashDataFrame.checksumDataFrame(df, 1) == HashDataFrame.checksumDataFrame(expectedDF, 1)
      )
    }
  }

  "FileSource" should {
    "read the version 0 of the delta table without filter" in {
      val strConfig: String =
        s"""
           |{
           | type: file-source
           | file-path: ${getClass.getResource("/input_test_data_delta").getPath}
           | format: delta
           | option-value: versionAsOf
           | version-or-time: 0
           |}""".stripMargin
      val config: InputSources = ConfigSource.fromConfig(ConfigFactory.parseString(strConfig))
        .loadOrThrow[InputSources]
      val df: DataFrame = config.loadData.sort("x", "y")
      val expectedDF: DataFrame = Seq(
        (0, 0),
        (0, 1),
        (0, 2),
        (0, 3),
        (1, 0),
        (1, 1),
        (1, 2),
        (1, 3),
        (3, 0),
        (3, 1),
        (3, 2),
        (3, 3),
        (2, 0),
        (2, 1),
        (2, 2),
        (2, 3)
      ).toDF("x", "y").sort("x", "y")

      assert(
        HashDataFrame.checksumDataFrame(df, 1) == HashDataFrame.checksumDataFrame(expectedDF, 1)
      )
    }
  }

  "FileSource exceptionCheck" should {
    "throw IllegalArgumentException when optionValue is defined but versionOrTime is not" in {
      val strConfig: String =
        s"""
           |{
           | type: file-source
           | file-path: ${getClass.getResource("/input_test_data_delta").getPath}
           | filter: "x = 2 AND y <= 2"
           | format: delta
           | option-value: versionAsOf
           |}""".stripMargin
      val config: InputSources = ConfigSource.fromConfig(ConfigFactory.parseString(strConfig))
        .loadOrThrow[InputSources]
      val thrown: IllegalArgumentException = the [IllegalArgumentException] thrownBy(config.loadData)

      assert(thrown.getMessage == "optionValue cannot be defined when versionOrTime is empty.")
    }
  }

  "FileSource exceptionCheck" should {
    "throw IllegalArgumentException when versionOrTime is defined but optionValue is not" in {
      val strConfig: String =
        s"""
           |{
           | type: file-source
           | file-path: ${getClass.getResource("/input_test_data_delta").getPath}
           | filter: "x = 2 AND y <= 2"
           | format: delta
           | version-or-time: 0
           |}""".stripMargin
      val config: InputSources = ConfigSource.fromConfig(ConfigFactory.parseString(strConfig))
        .loadOrThrow[InputSources]
      val thrown: IllegalArgumentException = the[IllegalArgumentException] thrownBy (config.loadData)

      assert(thrown.getMessage == "versionOrTime cannot be defined when optionValue is empty.")
    }
  }

  "BigQuerySource" should {
    "return a df after reading a BQ table with nested config test using faster execution" in {
      val strConfig: String =
        s"""
           |{
           | pipeline-name: BQ Test
           | data: {
           |  type: big-query-source
           |  query: "SELECT * FROM `invoicing-432408.bigspark_world.timesheets_normalised` LIMIT 10"
           |  dataset: bigspark_world
           |  project-id: "invoicing-432408"
           |  faster-execution: true
           |  }
           |}""".stripMargin
      case class Params(pipelineName: String, data: InputSources)
      val config: Params = ConfigSource.fromConfig(ConfigFactory.parseString(strConfig))
        .loadOrThrow[Params]
      val df: DataFrame = config.data.loadData

      assert(config.pipelineName == "BQ Test")
      assert(df.count == 10)
    }
  }

  "BigQuerySource" should {
    "return a df after reading a BQ table with nested config test using slower execution" in {
      val strConfig: String =
        s"""
           |{
           | pipeline-name: BQ Test
           | data: {
           |  type: big-query-source
           |  query: "SELECT * FROM `invoicing-432408.bigspark_world.timesheets_normalised` LIMIT 10"
           |  dataset: bigspark_world
           |  project-id: "invoicing-432408"
           |  }
           |}""".stripMargin
      case class Params(pipelineName: String, data: InputSources)
      val config: Params = ConfigSource.fromConfig(ConfigFactory.parseString(strConfig))
        .loadOrThrow[Params]
      val df: DataFrame = config.data.loadData

      assert(config.pipelineName == "BQ Test")
      assert(df.count == 10)
    }
  }
}
