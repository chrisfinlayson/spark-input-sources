package dev.bigspark.deframework.config

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig._
import pureconfig.generic.auto._

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

case class DpConfig(
  dataProductName: String,
  rawDataProductName: String,
  schema: Schema,
  columnSequenceOrder: List[String],
  metadata: Metadata,
  joinConditions: Option[List[JoinCondition]] // Add this line
)

case class JoinCondition(
  leftDataset: String,
  rightDataset: String,
  joinType: String,
  conditions: List[String]
)

trait ConfigReaderContract {
  def readSourceColumnsSchema(): DataFrame
  def readNewColumnsSchema(): DataFrame
  def readColumnDescriptionsMetadata(): Map[String, String]
  def readColumnSequenceOrder(): Seq[String]
  def readJoinConditions(): List[JoinCondition] // Add this line
}

class AppConfigReader(config: Config)(implicit spark: SparkSession) extends ConfigReaderContract {
  import spark.implicits._

  private val dpConfig: DpConfig = ConfigSource.fromConfig(config).loadOrThrow[DpConfig]

  private def convertCamelCaseToSnakeCase(columnName: String): String = {
    columnName.replaceAll("([A-Z])", "_$1").toLowerCase.stripPrefix("_")
  }

  private def renameColumns(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (acc, colName) =>
      acc.withColumnRenamed(colName, convertCamelCaseToSnakeCase(colName))
    }
  }

  override def readSourceColumnsSchema(): DataFrame = {
    renameColumns(dpConfig.schema.sourceColumns.toDF())
  }

  override def readNewColumnsSchema(): DataFrame = {
    renameColumns(dpConfig.schema.newColumns.toDF())
  }

  override def readColumnDescriptionsMetadata(): Map[String, String] = {
    dpConfig.metadata.columnDescriptions
  }

  override def readColumnSequenceOrder(): Seq[String] = {
    dpConfig.columnSequenceOrder
  }

  override def readJoinConditions(): List[JoinCondition] = {
    dpConfig.joinConditions.getOrElse(List.empty)
  }
}