package homework2.model

import java.sql.Timestamp

case class TaxiFact(
    VendorID: Integer,
    Tpep_Pickup_Datetime: Timestamp,
    Tpep_Dropoff_Datetime: Timestamp,
    Passenger_Count: Integer,
    Trip_Distance: Double,
    RatecodeID: Integer,
    Store_And_Fwd_Flag: String,
    PULocationID: Integer,
    DOLocationID: Integer,
    Payment_Type: Integer,
    Fare_Amount: Double,
    Extra: Double,
    Mta_Tax: Double,
    Tip_Amount: Double,
    Tolls_Amount: Double,
    Improvement_Surcharge: Double,
    Total_Amount: Double
)
