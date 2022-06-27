package homework2

import homework2.model.{TaxiFact, TaxiZone}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Properties
import scala.io.Source

object DataApiHomeWorkTaxi extends App {

  val spark = SparkSession
    .builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val driver                       = "org.postgresql.Driver"
  val url                          = "jdbc:postgresql://localhost:5432/otus"
  val user                         = "docker"
  val password                     = "docker"
  val path_yellow_taxi_jan_25_2018 = "src/main/resources/data/yellow_taxi_jan_25_2018"
  val path_taxi_zones              = "src/main/resources/data/taxi_zones.csv"
  val path_rdd_export_txt          = "out/rdd/result.txt"
  val path_to_sql_dll_taxi_trip    = "src/main/scala/homework2/sql/ddl.taxi_trip.sql"


  val taxiFactsDF = spark.read.load(path_yellow_taxi_jan_25_2018)
  val taxiZonesDF = spark.read.csv(path_taxi_zones)

  /**
   * RDD
   */
  import spark.implicits._
  val taxiFactsRDD: RDD[TaxiFact] = taxiFactsDF.as[TaxiFact].rdd

  val taxiZoneRDD = spark.sparkContext
    .textFile(path_taxi_zones)
    .map(l => l.split(","))
    .filter(t => t(3).toUpperCase() == t(3))
    .map(t => TaxiZone(t(0).toInt, t(1), t(2), t(3)))

  val mappedTaxiZoneRDD: RDD[(Int, String)] = taxiZoneRDD.map(z => (z.LocationID, z.Borough))
  val mappedTaxiFactsRDD: RDD[(Int, Int)]   = taxiFactsRDD.map(f => (f.DOLocationID, 1))

  var rdd_result_txt = ""

  val joinAndGroupedRDD: Unit =
    mappedTaxiFactsRDD
      .join(mappedTaxiZoneRDD)
      .map {
        case (_, (cnt, bor)) => (bor, cnt)
      }
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .foreach(x => {
        rdd_result_txt = rdd_result_txt + "\r\n" + x
      })

  println(rdd_result_txt)
  if (!Files.exists(Paths.get(path_rdd_export_txt))) {
    if (!Files.exists(Paths.get("out/rdd"))) {
      Files.createDirectory(Paths.get("out/rdd"))
    }
    Files.createFile(Paths.get(path_rdd_export_txt))
  }
  Files.write(Paths.get(path_rdd_export_txt), rdd_result_txt.getBytes(StandardCharsets.UTF_8))

  /**
   * DataFrame
   */

  val mostPopularLocations: Dataset[Row] = taxiFactsDF
    .groupBy("PULocationID")
    .count()
    .join(taxiZonesDF, taxiFactsDF("PULocationID") === taxiZonesDF("_c0"), "left")
    .withColumnRenamed("count", "c")
    .orderBy(desc("c"))
    .select(taxiZonesDF("*"))

  mostPopularLocations.show()
  val path = Paths.get("out/mostPopularLocations.parquet")
  mostPopularLocations.write.mode(SaveMode.Overwrite).parquet(path.toString)

  /**
   * DataSet
   */

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

  def readCSV(path: String)(spark: SparkSession):DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

  val taxiZonesDS: Dataset[TaxiZone] = readCSV(path_taxi_zones)(spark).as[TaxiZone]

  val resultDS = processTaxiData(taxiFactsDF.as[TaxiFact], taxiZonesDS)
  resultDS.show()

  /**
   * Подключение к PostgreSQL
   */
  val connProps = new Properties()
  connProps.put("user", user)
  connProps.put("password", password)

  resultDS.write.mode(SaveMode.Overwrite).jdbc(url,"taxi_trip",connProps)
}
