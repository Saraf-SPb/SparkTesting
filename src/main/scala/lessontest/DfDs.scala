package lessontest

import lessontest.model.{TaxiFact, TaxiZone}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object DfDs {

  def readParquet(path: String)(implicit spark: SparkSession): DataFrame = spark.read.load(path)

  def readCSV(path: String)(spark: SparkSession):DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

  def processTaxiData(taxiDS: Dataset[TaxiFact], taxiZonesDS: Dataset[TaxiZone]) = {
    taxiDS
      .join(taxiZonesDS, col("PULocationID") === col("LocationID"))
      .groupBy("Borough")
      .agg(
        count("*").as("total trips"),
        min("trip_distance").as("min distance"),
        round(mean("trip_distance"), 2).as("mean distance"),
        max("trip_distance").as("max distance")
      )
      .orderBy(col("total trips").desc)
  }



  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Taxi Big Data Application")
      .getOrCreate()

    val taxiFactsDF = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")

    import spark.implicits._
    val taxiZonesDS: Dataset[TaxiZone] = readCSV("src/main/resources/data/taxi_zones.csv")(spark).as[TaxiZone]
    val resultDS = processTaxiData(taxiFactsDF.as[TaxiFact], taxiZonesDS)
    resultDS.show()
  }
}
