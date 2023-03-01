
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
      .config("mysqlCon","mysql-connector-j-8.0.31.jar")
      .getOrCreate()

    val jdbcDF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "[TABLENAME]")
      .option("user", "[USERNAME]")
      .option("password", "[PASSWORD]")
      .load()

    jdbcDF
      .write
      .format("jdbc")
      .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "[TABLENAME]")
      .option("user", "[USERNAME]")
      .option("password", "[PASSWORD]")
      .save()



  }
}
// scalastyle:on println


