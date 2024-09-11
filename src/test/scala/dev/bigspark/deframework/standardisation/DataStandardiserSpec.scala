package dev.bigspark.deframework.standardisation

import dev.bigspark.SparkSessionWrapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import dev.bigspark.deframework.config.ConfigReaderContract

class DataStandardiserSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar with SparkSessionWrapper {

  val rawDpPath = "/Users/christopherfinlayson/dev/spark-input-sources/src/test/resources/raw_dp"
  val tempStdDpPath = "/Users/christopherfinlayson/dev/spark-input-sources/src/test/resources/std_dp_temp"
  val stdDpPath = "/Users/christopherfinlayson/dev/spark-input-sources/src/test/resources/std_dp"
  
  import spark.implicits._
  override def beforeAll(): Unit = {
    super.beforeAll()
    // Purge contents of raw_dp, std_dp_temp, and std_dp folders

    import org.apache.hadoop.fs.{FileSystem, Path}
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    
    def deleteFolder(path: String): Unit = {
      val hadoopPath = new Path(path)
      if (fs.exists(hadoopPath)) {
        fs.delete(hadoopPath, true)
      }
    }

    deleteFolder(rawDpPath)
    deleteFolder(tempStdDpPath)
    deleteFolder(stdDpPath)

    // Create a DataFrame with the sample data
    val sampleData = Seq(
      (9999, "john", 10, "ball", 100, "john@email.com"),
      (9976, "mary", 20, "kite", 200, "mary@email.com"),
      (8765, "ram", 330, "bat", 300, "ram@email.com"),
      (7654, "rahim", 400, "football", 40, "rahim@email.com"),
      (6543, "rita", 500, "badminton", 500, "rita@email.com")
    ).toDF("sup_id", "name", "price", "prod_name", "quantity", "email")

    // Write the DataFrame as a Delta table
    sampleData.write
      .format("delta")
      .mode("overwrite")
      .save(rawDpPath)

    // Create the Delta table
    spark.sql(s"CREATE TABLE IF NOT EXISTS raw_dp USING DELTA LOCATION '$rawDpPath'")
  }
  override def afterAll(): Unit = {
    spark.stop()
  }



  "createTempStdDpWithSourceColumns" should "create a temporary standardised table with source columns" in {

    val dataStandardiser = new DataStandardiser(spark, rawDpPath, tempStdDpPath, stdDpPath)

    val sourceColumnsSchema = Seq(
      ("sup_id", "Supplier_ID", "string", "CONCAT('SUP', '-' , sup_id)"),
      ("name", "Supplier_Name", "string", ""),
      ("price", "Purchase_Price", "int", ""),
      ("prod_name", "Product_Name", "string", ""),
      ("quantity", "Purchase_Quantity", "int", ""),
      ("", "Total_Cost", "int", "price * quantity")
    ).toDF("raw_name", "standardised_name", "data_type", "sql_transformation")

    dataStandardiser.createTempStdDpWithSourceColumns(sourceColumnsSchema)

    val resultDf = spark.sql(s"SELECT * FROM delta.`$tempStdDpPath`")
    resultDf.columns should contain allOf ("Supplier_ID", "Supplier_Name", "Purchase_Price", "Product_Name", "Purchase_Quantity", "Total_Cost")
  }

  "addNewColumnsInTempStdDp" should "add new columns to the temporary standardised table" in {
    val dataStandardiser = new DataStandardiser(spark, rawDpPath, tempStdDpPath, stdDpPath)

    val newColumnsSchema = Seq(
      ("Product_ID", "string", "MERGE INTO delta.`{temp_std_dp_path}` dest USING delta.`{temp_std_dp_path}` src ON dest.Product_Name = src.Product_Name WHEN MATCHED THEN UPDATE SET dest.Product_ID = src.Product_ID")
    ).toDF("name", "data_type", "sql_transformation")

    dataStandardiser.addNewColumnsInTempStdDp(newColumnsSchema)

    val resultDf = spark.sql(s"SELECT * FROM delta.`$tempStdDpPath`")
    resultDf.columns should contain ("Product_ID")
  }

  "updateColumnDescriptionsMetadata" should "update column descriptions metadata" in {
    val dataStandardiser = new DataStandardiser(spark, rawDpPath, tempStdDpPath, stdDpPath)

    val columnDescriptions = Map(
      "Supplier_ID" -> "Unique identifier for the supplier",
      "Supplier_Name" -> "Name of the supplier",
      "Purchase_Price" -> "Price at which the product was purchased",
      "Product_Name" -> "Name of the product",
      "Purchase_Quantity" -> "Quantity of the product purchased",
      "Total_Cost" -> "Total cost calculated as price * quantity",
      "Product_ID" -> "Unique identifier for the product"
    )

    dataStandardiser.updateColumnDescriptionsMetadata(columnDescriptions)

    val resultDf = spark.sql(s"DESCRIBE EXTENDED delta.`$tempStdDpPath`")
    
    println("Debug: Full DESCRIBE EXTENDED output:")
    resultDf.show(numRows = 1000, truncate = false)
    
    val commentRows = resultDf.filter($"col_name".isin("Supplier_ID", "Supplier_Name", "Purchase_Price", "Product_Name", "Purchase_Quantity", "Total_Cost", "Product_ID"))
    
    println("Debug: commentRows content:")
    commentRows.show(false)
    
    val mappedComments = commentRows.collect().map(row => {
      val colName = row.getString(0)
      val comment = row.getString(2)
      (colName, comment)
    })
    // Additional assertion to check if any descriptions were set
    mappedComments.length should be > 0

    mappedComments should contain allElementsOf columnDescriptions
  }

  "moveDataToStdDp" should "move data to the standardised table in the specified column order" in {
    val dataStandardiser = new DataStandardiser(spark, rawDpPath, tempStdDpPath, stdDpPath)

    val columnSequenceOrder = Seq("Supplier_ID", "Supplier_Name", "Purchase_Price", "Product_Name", "Purchase_Quantity", "Total_Cost", "Product_ID")

    dataStandardiser.moveDataToStdDp(columnSequenceOrder)

    val resultDf = spark.read.format("delta").load(stdDpPath)
    resultDf.columns shouldEqual columnSequenceOrder
  }

//  "run" should "execute the entire standardization process" in {
//    val dataStandardiser = new DataStandardiser(spark, rawDpPath, tempStdDpPath, stdDpPath)
//
//    val configReader = mock[ConfigReaderContract]
//
//    when(configReader.readSourceColumnsSchema()).thenReturn(Seq(
//      ("sup_id", "Supplier_ID", "string", "CONCAT('SUP', '-' , sup_id)"),
//      ("name", "Supplier_Name", "string", ""),
//      ("price", "Purchase_Price", "int", ""),
//      ("prod_name", "Product_Name", "string", ""),
//      ("quantity", "Purchase_Quantity", "int", ""),
//      ("", "Total_Cost", "int", "price * quantity")
//    ).toDF("raw_name", "standardised_name", "data_type", "sql_transformation"))
//
//    when(configReader.readNewColumnsSchema()).thenReturn(Seq(
//      ("Product_ID", "string", "MERGE INTO delta.`{temp_std_dp_path}` dest USING delta.`{temp_std_dp_path}` src ON dest.Product_Name = src.Product_Name WHEN MATCHED THEN UPDATE SET dest.Product_ID = src.Product_ID")
//    ).toDF("name", "data_type", "sql_transformation"))
//
//    when(configReader.readColumnDescriptionsMetadata()).thenReturn(Map(
//      "Supplier_ID" -> "Unique identifier for the supplier",
//      "Supplier_Name" -> "Name of the supplier",
//      "Purchase_Price" -> "Price at which the product was purchased",
//      "Product_Name" -> "Name of the product",
//      "Purchase_Quantity" -> "Quantity of the product purchased",
//      "Total_Cost" -> "Total cost calculated as price * quantity",
//      "Product_ID" -> "Unique identifier for the product"
//    ))
//
//    when(configReader.readColumnSequenceOrder()).thenReturn(Seq("Supplier_ID", "Supplier_Name", "Purchase_Price", "Product_Name", "Purchase_Quantity", "Total_Cost", "Product_ID"))
//
//    dataStandardiser.run(configReader)
//
//    val resultDf = spark.read.format("delta").load(stdDpPath)
//    resultDf.columns shouldEqual Seq("Supplier_ID", "Supplier_Name", "Purchase_Price", "Product_Name", "Purchase_Quantity", "Total_Cost", "Product_ID")
//  }
}
