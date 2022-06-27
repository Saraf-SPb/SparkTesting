package lessontest

import lessontest.DfDs.{processTaxiData, readCSV, readParquet}
import lessontest.model.{TaxiFact, TaxiZone}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{Dataset, Row}


class DfDsDsUnitTest extends SharedSparkSession {
  import testImplicits._

  test("join - processTaxiData") {
    val taxiDF2 = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val taxiZonesDF2: Dataset[TaxiZone] = readCSV("src/main/resources/data/taxi_zones.csv")(spark).as[TaxiZone]

    val actualDistribution = processTaxiData(taxiDF2.as[TaxiFact], taxiZonesDF2)

    checkAnswer(
      actualDistribution,
      Row("Manhattan",304266,0.0,2.23,66.0)  ::
        Row("Queens",17712,0.0,11.14,53.5) ::
        Row("Unknown",6644,0.0,2.34,42.8) ::
        Row("Brooklyn",3037,0.0,3.28,27.37) ::
        Row("Bronx",211,0.0,2.99,20.09) ::
        Row("EWR",19,0.0,3.46,17.3) ::
        Row("Staten Island",4,0.0,0.2,0.5) :: Nil
    )
  }

}

