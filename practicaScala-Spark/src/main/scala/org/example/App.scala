package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
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

  }

}
