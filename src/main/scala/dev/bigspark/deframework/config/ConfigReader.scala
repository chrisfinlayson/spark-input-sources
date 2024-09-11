package dev.bigspark.deframework.config

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

trait ConfigReaderContract {
  def readSourceColumnsSchema(): DataFrame

  def readNewColumnsSchema(): DataFrame

  def readColumnDescriptionsMetadata(): Map[String, String]

  def readColumnSequenceOrder(): Seq[String]
}

class ConfigReader(configPath: String)(implicit spark: SparkSession) extends ConfigReaderContract {
  private val configDf = spark.read.option("multiLine", true).json(configPath)

  override def readSourceColumnsSchema(): DataFrame = {
    val explodedDf = configDf.select(explode(col("schema.source_columns")).alias("source_columns"))
    explodedDf.selectExpr(
      "source_columns.raw_name as raw_name",
      "source_columns.standardised_name as standardised_name",
      "source_columns.data_type as data_type",
      "source_columns.sql_transformation as sql_transformation"
    )
  }

  override def readNewColumnsSchema(): DataFrame = {
    val explodedDf = configDf.select(explode(col("schema.new_columns")).alias("new_columns"))
    explodedDf.selectExpr(
      "new_columns.name as name",
      "new_columns.data_type as data_type",
      "new_columns.sql_transformation as sql_transformation"
    )
  }

  override def readColumnDescriptionsMetadata(): Map[String, String] = {
    val metadataDf = configDf.select("metadata.column_descriptions").collect()
    if (metadataDf.isEmpty) {
      Map.empty[String, String]
    } else {
      val row = metadataDf.head.getStruct(0)
      row.getValuesMap[String](row.schema.fieldNames)
    }
  }

  override def readColumnSequenceOrder(): Seq[String] = {
    configDf.first().getAs[Seq[String]]("column_sequence_order")
  }
}