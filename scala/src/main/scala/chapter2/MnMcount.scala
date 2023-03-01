// scalastyle:off println

package main.scala.chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro

/**
  * Usage: MnMcount <mnm_file_dataset>
  */
object MnMcount {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .getOrCreate()

    if (args.length < 1) {
      print("Usage: MnMcount <mnm_file_dataset>")
      sys.exit(1)
    }
    // get the M&M data set file name
    val mnmFile = args(0)
    // read the file into a Spark DataFrame
    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)
      .repartition(10)
    // display DataFrame
    mnmDF.show(5, false)

    // aggregate count of all colors and groupBy state and color
    // orderBy descending order
    val countMnMDF = mnmDF.select("State", "Color", "Count")
        .groupBy("State", "Color")
        .sum("Count")
        .orderBy(desc("sum(Count)"))

    // show all the resulting aggregation for all the dates and colors
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()

    // find the aggregate count for California by filtering
    val caCountMnNDF = mnmDF.select("*")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))

    // show the resulting aggregation for California
    caCountMnNDF.show(10)

    val stateByMaxMnMDF = mnmDF.select("*")
      .groupBy("State")
      .agg(max("Count"))
      .orderBy(desc("State"))

    stateByMaxMnMDF.show()

    val txCountMnNDF = mnmDF
      .where(col("State") === "TX"|| col("State")=== "NV")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))

    // show the resulting aggregation for California
    txCountMnNDF.show(10)

    val coByColorMnMDF = mnmDF.select("*")
      .groupBy("State")
      .agg(
        max("Count")as("max_Count"),
        min("Count")as("min_Count"),
        avg("Count")as("avg_Count"),
        count("Count")as("quantities"))
      .orderBy(desc("max_Count")).select()

    coByColorMnMDF.show()

    mnmDF.createOrReplaceTempView("mnmTable")

    spark.sql("SELECT State, MAX(Count) FROM mnmTable GROUP BY State ORDER BY State DESC").show()

    spark.sql("SELECT State, Color, SUM(Count) FROM mnmTable WHERE State = 'TX' OR State = 'NV' GROUP BY State, Color ORDER BY SUM(Count) DESC").show()

    spark.sql("SELECT State, MAX(Count) AS max_count, MIN(Count) AS min_count, AVG(Count) AS avg_count, COUNT(Count) AS quantity " +
      "FROM mnmTable GROUP BY State ORDER BY max_count DESC").show()

  //Chapter 3 Leer el CSV del ejemplo del cap2 y obtener la estructura del schema dado por defecto.

    println(mnmDF.printSchema)
    println(mnmDF.schema)

    //ii. ¿Cómo obtener el número de particiones de un DataFrame? R=1
    println(mnmDF.rdd.getNumPartitions)

    //iii. ¿Qué formas existen para modificar el número de particiones de un DataFrame?
    val asd = mnmDF.repartition(1)
    asd.count()
    println(asd.rdd.getNumPartitions)

    //Utilizando el mismo ejemplo utilizado en el capítulo para guardar en parquet y
    //guardar los datos en los formatos:
    //i. JSON
    val jsonPath = "C:/writeFiles/mnm_datasetJSON"
    mnmDF.write.format("json").save(jsonPath)
    //ii. CSV (dándole otro nombre para evitar sobrescribir el fichero origen)
    val csvPath = "C:/writeFiles/mnm_datasetCSV"
    mnmDF.write.format("csv").save(csvPath)
    //iii. AVRO
    val avroPath = "C:/writeFiles/mnm_datasetAVRO"
    mnmDF.write.format("avro").save(avroPath)

  }
}
// scalastyle:on println


