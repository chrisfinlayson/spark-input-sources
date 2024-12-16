package dev.bigspark.deframework.inputsources

import org.apache.spark.sql.{DataFrame, SparkSession}

trait InputSource {
  def read(): DataFrame
  def write(df: DataFrame): Unit
  def registerTable: Unit
  def getTableName: String
}

class FileSource(spark: SparkSession, path: String, tableName: String) extends InputSource {
  override def read(): DataFrame = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING DELTA LOCATION '$path'")
    spark.read.format("delta").load(path)
  }
  override def registerTable: Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING DELTA LOCATION '$path'")
  }

  override def write(df: DataFrame): Unit = {
    df.write.format("delta").mode("overwrite").save(path)
  }

  override def getTableName: String = tableName
}