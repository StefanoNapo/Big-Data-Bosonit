package org.example

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, desc, length, round, sum, trim}
/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("SparkByExample.com")
      .getOrCreate()

    val csvFile = "estadisticas202212.csv"

    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .load(csvFile)

    df.show()

    // df.select(lista.map(col):_*).show()
    val df_trimmed = df.select(
      col("COD_DISTRITO"),
      trim(col("DESC_DISTRITO")).as("DESC_DISTRITO"),
      col("COD_DIST_BARRIO"),
      col("COD_BARRIO"),
      trim(col("DESC_BARRIO")).as("DESC_BARRIO"),
      col("COD_DIST_SECCION"),
      col("COD_SECCION"),
      col("COD_EDAD_INT"),
      col("ESPANOLESHOMBRES"),
      col("ESPANOLESMUJERES"),
      col("EXTRANJEROSHOMBRES"),
      col("EXTRANJEROSMUJERES"),
      col("FX_CARGA"),
      col("FX_DATOS_INI"),
      col("FX_DATOS_FIN")
      )

    df_trimmed.show()

    val df_final = df_trimmed.na.fill(0)

    df_final.show()

    val df_barrios = df_final.select(
      col("COD_BARRIO"),
      col("DESC_BARRIO")
    ).distinct().orderBy(col("DESC_BARRIO"))

    df_barrios.show(150)

    df_final.createOrReplaceTempView("padron")

    spark.sql("SELECT count(DISTINCT DESC_BARRIO) AS diferentes_barrios FROM padron").show()

    val df_final_len = df_final.withColumn("longitud", length(col("DESC_DISTRITO")))

    df_final_len.show()

    val df_final_no_len = df_final_len.drop(col("longitud"))

    df_final_no_len.show()

    val df_final_part = df_final_no_len.repartition(col("DESC_DISTRITO"),col("DESC_BARRIO"))

    df_final_part.show()

    println(df_final_part.rdd.getNumPartitions)

    val df_cached = df_final_part.cache()

    df_cached.show()

    val df_sums = df_cached.groupBy(col("DESC_DISTRITO"),col("DESC_BARRIO"))
      .agg(sum(col("ESPANOLESHOMBRES")).alias("sum_esp_hom"),
        sum(col("ESPANOLESMUJERES")).alias("sum_esp_muj"),
        sum(col("EXTRANJEROSHOMBRES")).alias("sum_ext_hom"),
        sum(col("EXTRANJEROSMUJERES")).alias("sum_ext_muj"))
      .sort(desc("sum_ext_muj"), desc("sum_ext_hom"))

    df_sums.show(30)

    val df_desc = df_final.groupBy(col("DESC_DISTRITO"),col("DESC_BARRIO"))
      .agg(sum(col("ESPANOLESHOMBRES")).alias("sum_esp_hom")).orderBy("DESC_DISTRITO", "DESC_BARRIO")

    df_desc.show()

    val df_joined = df_desc.join(df_final, (df_final.col("DESC_BARRIO") === df_desc.col("DESC_BARRIO"))
      && (df_final.col("DESC_DISTRITO") === df_desc.col("DESC_DISTRITO")), "inner")

    df_joined.show()

    val windowSpec = Window.partitionBy(col("DESC_DISTRITO"),col("DESC_BARRIO")).orderBy(col("DESC_DISTRITO"),col("DESC_BARRIO"))

    val df_window = df_final.withColumn("sum_esp_hom", sum(col("ESPANOLESHOMBRES")).over(windowSpec))

    df_window.show()

    val df_contingencia = df_final.filter(
      (df_final.col("DESC_DISTRITO") === "CENTRO") ||
        (df_final.col("DESC_DISTRITO") === "BARAJAS") ||
        (df_final.col("DESC_DISTRITO") === "RETIRO"))
    .groupBy(col("COD_EDAD_INT"))
    .pivot(col("DESC_DISTRITO"))
    .agg(sum(col("ESPANOLESMUJERES")).alias("sum_ext_muj"))
    .orderBy(col("COD_EDAD_INT"))

    df_contingencia.show()


    val df_muj_edad_porc = df_contingencia
    .withColumn("BARAJAS PERC", round(col("BARAJAS") / (col("CENTRO") + col("BARAJAS") + col("RETIRO")) * 100, 2))
    .withColumn("CENTRO PERC", round(col("CENTRO") / (col("CENTRO") + col("BARAJAS") + col("RETIRO")) * 100, 2))
    .withColumn("RETIRO PERC", round(col("RETIRO") / (col("CENTRO") + col("BARAJAS") + col("RETIRO")) * 100, 2))

    df_muj_edad_porc.show()


    df_final.write.partitionBy("COD_DISTRITO", "COD_BARRIO").csv("padron-csv-sc")

    df_final.write.partitionBy("COD_DISTRITO", "COD_BARRIO").save("padron.parquet-sc")

    Thread.sleep(15000)

    df_cached.unpersist()

    Thread.sleep(45000)


  }
}
