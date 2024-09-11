package dev.bigspark.deframework.standardisation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import dev.bigspark.deframework.config.ConfigReaderContract

class DataStandardiserSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("DataStandardiserSpec")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def afterAll(): Unit = {
    spark.stop()
  }
  val rawDpPath = "/Users/christopherfinlayson/dev/spark-input-sources/src/test/resources/raw_dp"
  val tempStdDpPath = "/Users/christopherfinlayson/dev/spark-input-sources/src/test/resources/std_dp_temp"
  val stdDpPath = "/Users/christopherfinlayson/dev/spark-input-sources/src/test/resources/std_dp"


  "createTempStdDpWithSourceColumns" should "create a temporary standardised table with source columns" in {

    val dataStandardiser = new DataStandardiser(rawDpPath, tempStdDpPath, stdDpPath)

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
    val dataStandardiser = new DataStandardiser(rawDpPath, tempStdDpPath, stdDpPath)

    val newColumnsSchema = Seq(
      ("Product_ID", "string", "MERGE INTO delta.`{temp_std_dp_path}` dest USING delta.`dbfs:/FileStore/project/Product` src ON dest.Product_Name = src.Product_Name WHEN MATCHED THEN UPDATE SET dest.Product_ID = src.Product_ID")
    ).toDF("name", "data_type", "sql_transformation")

    dataStandardiser.addNewColumnsInTempStdDp(newColumnsSchema)

    val resultDf = spark.sql(s"SELECT * FROM delta.`$tempStdDpPath`")
    resultDf.columns should contain ("Product_ID")
  }

  "updateColumnDescriptionsMetadata" should "update column descriptions metadata" in {

    val dataStandardiser = new DataStandardiser(rawDpPath, tempStdDpPath, stdDpPath)

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

    val resultDf = spark.sql(s"DESCRIBE TABLE delta.`$tempStdDpPath`")
    resultDf.collect().map(row => (row.getString(0), row.getString(1))) should contain allOf (
      ("Supplier_ID", "Unique identifier for the supplier"),
      ("Supplier_Name", "Name of the supplier"),
      ("Purchase_Price", "Price at which the product was purchased"),
      ("Product_Name", "Name of the product"),
      ("Purchase_Quantity", "Quantity of the product purchased"),
      ("Total_Cost", "Total cost calculated as price * quantity"),
      ("Product_ID", "Unique identifier for the product")
    )
  }

  "moveDataToStdDp" should "move data to the standardised table in the specified column order" in {
    val dataStandardiser = new DataStandardiser(rawDpPath, tempStdDpPath, stdDpPath)

    val columnSequenceOrder = Seq("Supplier_ID", "Supplier_Name", "Purchase_Price", "Product_Name", "Purchase_Quantity", "Total_Cost", "Product_ID")

    dataStandardiser.moveDataToStdDp(columnSequenceOrder)

    val resultDf = spark.read.format("delta").load(stdDpPath)
    resultDf.columns shouldEqual columnSequenceOrder
  }

  "run" should "execute the entire standardization process" in {
    val dataStandardiser = new DataStandardiser(rawDpPath, tempStdDpPath, stdDpPath)

    val configReader = mock[ConfigReaderContract]

    when(configReader.readSourceColumnsSchema()).thenReturn(Seq(
      ("sup_id", "Supplier_ID", "string", "CONCAT('SUP', '-' , sup_id)"),
      ("name", "Supplier_Name", "string", ""),
      ("price", "Purchase_Price", "int", ""),
      ("prod_name", "Product_Name", "string", ""),
      ("quantity", "Purchase_Quantity", "int", ""),
      ("", "Total_Cost", "int", "price * quantity")
    ).toDF("raw_name", "standardised_name", "data_type", "sql_transformation"))

    when(configReader.readNewColumnsSchema()).thenReturn(Seq(
      ("Product_ID", "string", "MERGE INTO delta.`{temp_std_dp_path}` dest USING delta.`dbfs:/FileStore/project/Product` src ON dest.Product_Name = src.Product_Name WHEN MATCHED THEN UPDATE SET dest.Product_ID = src.Product_ID")
    ).toDF("name", "data_type", "sql_transformation"))

    when(configReader.readColumnDescriptionsMetadata()).thenReturn(Map(
      "Supplier_ID" -> "Unique identifier for the supplier",
      "Supplier_Name" -> "Name of the supplier",
      "Purchase_Price" -> "Price at which the product was purchased",
      "Product_Name" -> "Name of the product",
      "Purchase_Quantity" -> "Quantity of the product purchased",
      "Total_Cost" -> "Total cost calculated as price * quantity",
      "Product_ID" -> "Unique identifier for the product"
    ))

    when(configReader.readColumnSequenceOrder()).thenReturn(Seq("Supplier_ID", "Supplier_Name", "Purchase_Price", "Product_Name", "Purchase_Quantity", "Total_Cost", "Product_ID"))

    dataStandardiser.run(configReader)

    val resultDf = spark.read.format("delta").load(stdDpPath)
    resultDf.columns shouldEqual Seq("Supplier_ID", "Supplier_Name", "Purchase_Price", "Product_Name", "Purchase_Quantity", "Total_Cost", "Product_ID")
  }
}
