package lessontest

import lessontest.model.{TaxiFact, TaxiZone}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.rdd.RDD

object Rdd {
  /**
   * Чтение из parquet
   * @param path
   * @param spark
   * @return
   */
  def readParquet(path: String)(implicit spark: SparkSession): DataFrame = spark.read.load(path)

  /**
   * Чтение из csv
   * @param path
   * @param spark
   * @return
   */
  def readCSV(path: String)(implicit spark: SparkSession): RDD[TaxiZone] =
    spark.sparkContext
      .textFile(path)
      .map(l => l.split(","))
      .filter(t => t(3).toUpperCase() == t(3))
      .map(t => TaxiZone(t(0).toInt, t(1), t(2), t(3)))

  /**
   * Расчет статистики
   * @param taxiZoneRDD
   * @param taxiFactsDF
   * @param spark
   * @return
   */
  def processTaxiDataRDD(
      taxiZoneRDD: RDD[TaxiZone],
      taxiFactsDF: DataFrame
  )(implicit spark: SparkSession): Array[(String, Int)] = {
    import spark.implicits._
    val taxiFactsRDD: RDD[TaxiFact] = taxiFactsDF.as[TaxiFact].rdd

    val mappedTaxiZoneRDD: RDD[(Int, String)] = taxiZoneRDD.map(z => (z.LocationID, z.Borough))
    val mappedTaxiFactsRDD: RDD[(Int, Int)]   = taxiFactsRDD.map(f => (f.DOLocationID, 1))

    val joinAndGroupedRDD = mappedTaxiFactsRDD
      .join(mappedTaxiZoneRDD)
      .map {
        case (_, (cnt, bor)) => (bor, cnt)
      }
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .collect()

    joinAndGroupedRDD
  }

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("Joins")
      .config("spark.master", "local")
      .getOrCreate()

    val taxiZonesDF2 = readCSV("src/main/resources/data/taxi_zones.csv")
    val taxiFactsDF2 = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")

    val value = processTaxiDataRDD(taxiZonesDF2, taxiFactsDF2)
    value.map(x => { println(x) })
  }
}
