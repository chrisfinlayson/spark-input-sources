package dev.bigspark.deframework.config

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigReaderSpec extends AnyFlatSpec with Matchers {
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("ConfigReaderSpec")
    .getOrCreate()

  import spark.implicits._

  "ConfigReader" should "read source columns schema correctly" in {
    val configPath = getClass.getResource("/test_config.json").getPath
    val configReader = new ConfigReader(configPath)
    
    val sourceColumnsSchema = configReader.readSourceColumnsSchema()
    sourceColumnsSchema.count() should be > 0L
    sourceColumnsSchema.columns should contain allOf("raw_name", "standardised_name", "data_type", "sql_transformation")
  }

  it should "read new columns schema correctly" in {
    val configPath = getClass.getResource("/test_config.json").getPath
    val configReader = new ConfigReader(configPath)
    
    val newColumnsSchema = configReader.readNewColumnsSchema()
    newColumnsSchema.count() should be > 0L
    newColumnsSchema.columns should contain allOf("name", "data_type", "sql_transformation")
  }

  it should "read column descriptions metadata correctly" in {
    val configPath = getClass.getResource("/test_config.json").getPath
    val configReader = new ConfigReader(configPath)
    
    val columnDescriptions = configReader.readColumnDescriptionsMetadata()
    columnDescriptions should not be empty
    columnDescriptions should contain key "Supplier_ID"
  }

  it should "read column sequence order correctly" in {
    val configPath = getClass.getResource("/test_config.json").getPath
    val configReader = new ConfigReader(configPath)
    
    val columnSequenceOrder = configReader.readColumnSequenceOrder()
    columnSequenceOrder should not be empty
    columnSequenceOrder should contain("Supplier_ID")
  }
}