package dev.bigspark.deframework.standardisation

import dev.bigspark.SparkSessionWrapper
import dev.bigspark.deframework.config.ConfigReaderContract
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class DataStandardiser (spark: SparkSession, rawDpPath: String, tempStdDpPath: String, stdDpPath: String){

  def createTempStdDpWithSourceColumns(sourceColumnsSchema: DataFrame): Unit = {
    sourceColumnsSchema.createOrReplaceTempView("source_columns_config_table")
    val selectQuerySql = s"""
      SELECT 
        concat(
          "SELECT ", 
          array_join(collect_list(select_expression), ", "), 
          " FROM delta.`$rawDpPath`"
        ) as select_query 
      FROM (
        SELECT 
          CASE
            WHEN sql_transformation = "" THEN concat("CAST(", concat("`", raw_name, "`"), " AS ", data_type, ") AS ", standardised_name)
            ELSE concat("CAST(", sql_transformation, " AS ", data_type, ") AS ", standardised_name)
          END as select_expression 
        FROM source_columns_config_table
      )
    """
    val df = spark.sql(selectQuerySql)
    val selectQuery = df.first().getAs[String]("select_query")
    println(selectQuery)
    val tempData = spark.sql(selectQuery)
    
    // Delete the existing Delta table if it exists
    DeltaTable.forName("raw_dp").delete()
    
    // Write the new data to the Delta table
    tempData.write
      .format("delta")
      .mode("overwrite")
      .save(tempStdDpPath)
    
    spark.sql(s"CREATE TABLE IF NOT EXISTS delta.`$tempStdDpPath` USING DELTA LOCATION '$tempStdDpPath'")
  }

  def addNewColumnsInTempStdDp(newColumnsSchema: DataFrame): Unit = {
    newColumnsSchema.collect().foreach { row =>
      val addNewColumnsSql = s"ALTER TABLE delta.`$tempStdDpPath` ADD COLUMN ${row.getAs[String]("name")} ${row.getAs[String]("data_type")}"
      val sqlTransformation = row.getAs[String]("sql_transformation").replace("{temp_std_dp_path}", tempStdDpPath)
      spark.sql(addNewColumnsSql)
      spark.sql(sqlTransformation)
    }
  }

  def updateColumnDescriptionsMetadata(columnDescriptions: Map[String, String]): Unit = {
    val alterTableStatements = columnDescriptions.map { case (colName, description) =>
      s"ALTER TABLE delta.`$tempStdDpPath` ALTER COLUMN `$colName` COMMENT '$description'"
    }
    alterTableStatements.foreach(spark.sql)
  }

  def moveDataToStdDp(columnSequenceOrder: Seq[String]): Unit = {
    val tempStdDf = spark.read.format("delta").load(tempStdDpPath)
    val orderedDf = tempStdDf.select(columnSequenceOrder.map(col): _*)
    orderedDf.write.option("mergeSchema", "true").format("delta").mode("overwrite").save(stdDpPath)
  }

  def run(configReader: ConfigReaderContract): Unit = {
    println("Raw df : ")
    val rawDf = spark.read.format("delta").load(rawDpPath)
    rawDf.show()

    val sourceColumnsSchema = configReader.readSourceColumnsSchema()
    createTempStdDpWithSourceColumns(sourceColumnsSchema)

    val newColumnsSchema = configReader.readNewColumnsSchema()
    addNewColumnsInTempStdDp(newColumnsSchema)

    val columnDescriptions = configReader.readColumnDescriptionsMetadata()
    updateColumnDescriptionsMetadata(columnDescriptions)

    val columnSequenceOrder = configReader.readColumnSequenceOrder()
    moveDataToStdDp(columnSequenceOrder)

    println("Standardised df : ")
    val stdDf = spark.read.format("delta").load(stdDpPath)
    stdDf.show()

    println("Schema information for Standardised df : ")
    stdDf.printSchema()
    spark.sql(s"DESCRIBE TABLE delta.`$stdDpPath`").show()
  }
}