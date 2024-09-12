package dev.bigspark.deframework.config

import pureconfig._
import pureconfig.generic.auto._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class SourceColumn(
  rawName: String,
  standardisedName: String,
  dataType: String,
  sqlTransformation: String
)

case class NewColumn(
  name: String,
  dataType: String,
  sqlTransformation: String
)

case class Schema(
  sourceColumns: List[SourceColumn],
  newColumns: List[NewColumn]
)

case class Metadata(
  columnDescriptions: Map[String, String]
)

case class Config(
  dataProductName: String,
  rawDataProductName: String,
  schema: Schema,
  columnSequenceOrder: List[String],
  metadata: Metadata
)

trait ConfigReaderContract {
  def readSourceColumnsSchema(): DataFrame
  def readNewColumnsSchema(): DataFrame
  def readColumnDescriptionsMetadata(): Map[String, String]
  def readColumnSequenceOrder(): Seq[String]
}

class ConfigReader(configPath: String)(implicit spark: SparkSession) extends ConfigReaderContract {
  private val config = ConfigSource.file(configPath).loadOrThrow[Config]

  import spark.implicits._

  private def convertCamelCaseToSnakeCase(columnName: String): String = {
    columnName.replaceAll("([A-Z])", "_$1").toLowerCase.stripPrefix("_")
  }

  private def renameColumns(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (acc, colName) =>
      acc.withColumnRenamed(colName, convertCamelCaseToSnakeCase(colName))
    }
  }

  override def readSourceColumnsSchema(): DataFrame = {
    renameColumns(config.schema.sourceColumns.toDF())
  }

  override def readNewColumnsSchema(): DataFrame = {
    renameColumns(config.schema.newColumns.toDF())
  }

  override def readColumnDescriptionsMetadata(): Map[String, String] = {
    config.metadata.columnDescriptions
  }

  override def readColumnSequenceOrder(): Seq[String] = {
    config.columnSequenceOrder
  }
}