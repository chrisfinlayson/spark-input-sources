package dev.bigspark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  lazy val spark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    SparkSession
      .builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.log.level", "ERROR")
      .config("spark.sql.session.timeZone", "UTC")
      .master("local[*]")
      .getOrCreate()

  }
}
