package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, bround, col, when}
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

    val df_precio_to_double = df_tabla.withColumn("precio", when(col("precio") === "7,61", "7.61")
      .when(col("precio") === "11,52", "11.52").otherwise(col("precio"))
      .cast("double"))

    df_precio_to_double.show()

    val df_categoria_correcta = df_precio_to_double
      .withColumn("categoría", when(col("categoría") === "tecnologia", "tecnología")
        .when(col("categoría") === "Oficina", "oficina")
        .otherwise(col("categoría")))

    df_categoria_correcta.show()

    val df_precio_por_cat = df_categoria_correcta.groupBy("categoría").agg(avg(col("precio")))

    df_precio_por_cat.show()

    val df_limited_decimal = df_categoria_correcta
      .withColumn("precio", bround(df_categoria_correcta("precio"), 2))

    df_limited_decimal.show()

    val df_premium_join = df_limited_decimal
      .join(df_tabla_premium, df_tabla_premium.col("Categoría") === df_limited_decimal.col("Categoría"))

    df_premium_join.show()

    val df_premium_avg = df_premium_join.filter(col("Premium") === "YES").agg(avg("precio"))

    df_premium_avg.show()

    df_tabla.printSchema()

    val tabla_spark_plus_col_path = "tabla_spark_plus_col.csv"

    val df_tabla_plus_col = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .load(tabla_spark_plus_col_path)

    df_tabla_plus_col.show()

    val df_precio_to_double_plus = df_tabla_plus_col.withColumn("precio", when(col("precio") === "7,61", "7.61")
      .when(col("precio") === "11,52", "11.52").otherwise(col("precio"))
      .cast("double"))

    df_precio_to_double.show()

    val df_categoria_correcta_plus = df_precio_to_double_plus
      .withColumn("categoría", when(col("categoría") === "tecnologia", "tecnología")
        .when(col("categoría") === "Oficina", "oficina")
        .otherwise(col("categoría")))

    df_categoria_correcta_plus.show()

    val df_precio_segun_campo = df_categoria_correcta_plus
      .withColumn("precio_correcto", when(col("Campo_precio") === "precio", col("precio"))
        .when(col("Campo_precio") === "Precio_premium", col("Precio_premium"))
//        .when(col("Campo_precio") === "Estándar", col("Estándar"))
//        .when(col("Campo_precio") === "Gold", col("Gold"))
      )

    val df_precio_por_cat_plus = df_precio_segun_campo.groupBy("categoría").agg(avg(col("precio_correcto"))as("avg_precio"))

    df_precio_por_cat_plus.show()

    val df_limited_decimal_plus = df_precio_por_cat_plus
      .withColumn("precio", bround(df_precio_por_cat_plus("precio"), 2))

    df_limited_decimal_plus.show()

    val df_premium_join_plus = df_limited_decimal_plus
      .join(df_tabla_premium, df_tabla_premium.col("Categoría") === df_limited_decimal_plus.col("Categoría"))

    df_premium_join_plus.show()

    val df_premium_avg_plus = df_premium_join_plus.filter(col("Premium") === "YES").agg(avg("precio"))

    df_premium_avg_plus.show()

    df_precio_por_cat_plus.show()
  }

}

