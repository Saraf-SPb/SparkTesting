package lessontest

import lessontest.Rdd.{processTaxiDataRDD, readCSV, readParquet}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class RddUnitTest extends AnyFlatSpec {

  implicit val spark = SparkSession
    .builder()
    .config("spark.master", "local")
    .appName("Test RDD")
    .getOrCreate()

  it should "upload and process data" in {
    val taxiZonesDF2 = readCSV("src/main/resources/data/taxi_zones.csv")
    val taxiDF2      = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")

    val actualDistribution = processTaxiDataRDD(taxiZonesDF2, taxiDF2).map(x => {x._2})

    assert(actualDistribution(0) == 292020)
    assert(actualDistribution(1) == 12686)
    assert(actualDistribution(2) == 12654)
    assert(actualDistribution(3) == 6714)
    assert(actualDistribution(4) == 1589)
    assert(actualDistribution(5) == 508)
    assert(actualDistribution(6) == 64)
  }

}
