
package main.scala.chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro

/**
  * Usage: MnMcount <mnm_file_dataset>
  */
object MnMcount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkSQLExampleApp")
      .getOrCreate()

    val csvFile = "departuredelays.csv"

    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)

    df.createOrReplaceTempView("us_delay_flights_tbl")

    spark.sql(
      """SELECT distance, origin, destination
  FROM us_delay_flights_tbl WHERE distance > 1000
  ORDER BY distance DESC""").show(10)

    spark.sql(
      """SELECT date, delay, origin, destination
  FROM us_delay_flights_tbl
  WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
  ORDER by delay DESC""").show(10)

    spark.sql(
      """SELECT delay, origin, destination,
     CASE
     WHEN delay > 360 THEN 'Very Long Delays'
     WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
     WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
     WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
     WHEN delay = 0 THEN 'No Delays'
     ELSE 'Early'
     END AS Flight_Delays
     FROM us_delay_flights_tbl
     ORDER BY origin, delay DESC""").show(10)

    val distantMore1000 = df
      .where(col("distance") > 1000)
      .orderBy(desc("distance"))
      .select("distance","origin", "destination")

    distantMore1000.show(10)

    val delaySFOtoORD = df
      .where((col("delay") > 120) && (col("origin") ===  "SFO") && (col("destination") ===  "ORD"))
      .orderBy(desc("distance"))
      .select("distance", "origin", "destination")

    delaySFOtoORD.show(10)

    val delaysToText = df.withColumn("Flight_Delays",
      when(col("delay") > 360, "Very Long Delays")
        .when((col("delay") > 120) && (col("delay") < 360), "Long Delays")
        .when((col("delay") > 60) && (col("delay") < 120), "Short Delays")
        .when((col("delay") > 0) && (col("delay") < 60), "Tolerable Delays")
        .when(col("delay") === 0, "No Delays")
        .otherwise("Early"))
      .orderBy(desc("origin"), desc("delay"))
      .select("*")

    delaysToText.show(10)

    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    spark.catalog.listDatabases()
    spark.catalog.listTables()

    val usFlightsDF = spark.sql("SELECT * FROM us_delay_flights_tbl")
    val usFlightsDF2 = spark.table("us_delay_flights_tbl")

    usFlightsDF.show()
    usFlightsDF2.show()


    val file = "C:/writeFiles/mnm_datasetJSON/*"
    val dfmnmjson = spark.read.format("json").load(file)
    dfmnmjson.show(10, false)

    spark.sql("CREATE OR REPLACE TEMPORARY VIEW mnm_json USING json OPTIONS (path " +
    "'C:/writeFiles/mnm_datasetJSON/*')")
    spark.sql("SELECT * FROM mnm_json").show()

    val file2 = "C:/writeFiles/mnm_datasetCSV/*"
    val dfmnmcsv = spark.read.format("csv").load(file)
    dfmnmcsv.show(10, false)

    spark.sql("CREATE OR REPLACE TEMPORARY VIEW mnm_csv USING csv OPTIONS (path " +
      "'C:/writeFiles/mnm_datasetCSV/*')")
    spark.sql("SELECT * FROM mnm_csv").show()

  }
}
// scalastyle:on println


