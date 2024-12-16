package dev.bigspark.deframework.standardisation

import dev.bigspark.deframework.config.AppConfigReader
import dev.bigspark.deframework.inputsources.InputSource
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class DataStandardiser(spark: SparkSession, rawDpSource: InputSource, tempStdDpSource: InputSource, stdDpSource: InputSource, configReader: AppConfigReader) {

  def createTempStdDpWithSourceColumns(): Unit = {
    val sourceColumnsSchema = configReader.readSourceColumnsSchema()
    sourceColumnsSchema.createOrReplaceTempView("source_columns_config_table")
    val selectQuerySql = s"""
      SELECT 
        concat(
          "SELECT ", 
          array_join(collect_list(select_expression), ", "), 
          " FROM ", "${rawDpSource.getTableName}"
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
    
    tempStdDpSource.write(tempData)
  }

  def addNewColumnsInTempStdDp(): Unit = {
    val newColumnsSchema = configReader.readNewColumnsSchema()
    val tempStdDpTableName = tempStdDpSource.getTableName
    newColumnsSchema.collect().foreach { row =>
      val addNewColumnsSql = s"ALTER TABLE $tempStdDpTableName ADD COLUMN ${row.getAs[String]("name")} ${row.getAs[String]("data_type")}"
      val sqlTransformation = row.getAs[String]("sql_transformation").replace("{temp_std_dp_path}", tempStdDpTableName)
      spark.sql(addNewColumnsSql)
      println("Debug:"+addNewColumnsSql)
      spark.sql("SHOW TABLES").show()
      println("Debug:"+sqlTransformation)
      spark.sql(sqlTransformation)
    }
  }

  def updateColumnDescriptionsMetadata(): Unit = {
    val columnDescriptions = configReader.readColumnDescriptionsMetadata()
    val tempStdDpTableName = tempStdDpSource.getTableName
    val alterTableStatements = columnDescriptions.map { case (colName, description) =>
      s"ALTER TABLE $tempStdDpTableName ALTER COLUMN `$colName` COMMENT '$description'"
    }
    alterTableStatements.foreach(spark.sql)
  }

  def moveDataToStdDp(): Unit = {
    val columnSequenceOrder = configReader.readColumnSequenceOrder()
    val tempStdDf = tempStdDpSource.read()
    val orderedDf = tempStdDf.select(columnSequenceOrder.map(col): _*)
    stdDpSource.write(orderedDf)
  }

  def run(): Unit = {
    println("Raw df : ")
    val rawDf = rawDpSource.read()
    rawDf.show()

    createTempStdDpWithSourceColumns()
    addNewColumnsInTempStdDp()
    updateColumnDescriptionsMetadata()
    moveDataToStdDp()

    println("Standardised df : ")
    val stdDf = stdDpSource.read()
    stdDf.show()

    println("Schema information for Standardised df : ")
    stdDf.printSchema()
    spark.sql(s"DESCRIBE TABLE ${stdDpSource.getTableName}").show()
  }

  def performJoins(datasets: Map[String, DataFrame]): DataFrame = {
    val joinConditions = configReader.readJoinConditions()
    
    joinConditions.foldLeft(datasets(joinConditions.head.leftDataset)) { (accDF, joinCondition) =>
      val rightDF = datasets(joinCondition.rightDataset)
      accDF.join(
        rightDF,
        expr(joinCondition.conditions.mkString(" AND ")),
        joinCondition.joinType
      )
    }
  }
}