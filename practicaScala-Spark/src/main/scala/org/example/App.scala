package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col}
/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("Scala-Spark")
      .getOrCreate()

    val rdd:RDD[Int] = spark.sparkContext.parallelize(List(1,4,5,6,7))

    val rddMapped = rdd.map(x => (x, x%2))

    rddMapped.collect().foreach(println)

    val rddOdd = rddMapped.filter(_._2 == 1)

    rddOdd.collect().foreach(println)

    val rrdSum = rddOdd.keys.reduce(_+_)

    println(rrdSum)

    val rddReducedByKey = rddMapped.map(_.swap).reduceByKey(_+_)

    rddReducedByKey.collect().foreach(println)

    val tabla_spark_path = "tabla_spark.csv"

    val tabla_premium_path = "tabla_premium_spark.csv"

    val df_tabla = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .load(tabla_spark_path)

    val df_tabla_premium = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .load(tabla_premium_path)

    df_tabla.show()

    df_tabla_premium.show()

    val df_precio_por_cat = df_tabla.groupBy("categoría").agg(avg(col("precio")))

    df_precio_por_cat.show()

    df_tabla.printSchema()
  }

}

