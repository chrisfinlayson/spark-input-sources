package dev.bigspark

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .master("local[*]")
      .getOrCreate()
  }
}
