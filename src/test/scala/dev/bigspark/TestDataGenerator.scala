import org.apache.spark.sql.{DataFrame, SparkSession}

object TestDataGenerator {
  def generateTestData(spark: SparkSession): Map[String, DataFrame] = {
    import spark.implicits._

    val mainData = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    ).toDF("id", "name")

    val lookupData = Seq(
      (1, "Engineer"),
      (2, "Manager"),
      (4, "Designer")
    ).toDF("id", "job")

    Map(
      "main" -> mainData,
      "lookup" -> lookupData
    )
  }
}