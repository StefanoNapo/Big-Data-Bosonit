import time

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, trim, length, sum, desc, filter, round

spark = (SparkSession
         .builder
         .appName("Padron6")
         .getOrCreate())

csv_file = "estadisticas202212.csv"

df = (spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .load(csv_file))

df.show()

df_trimmed = df.select(
    "COD_DISTRITO",
    trim("DESC_DISTRITO").alias("DESC_DISTRITO"),
    "COD_DIST_BARRIO",
    trim("DESC_BARRIO").alias("DESC_BARRIO"),
    "COD_BARRIO",
    "COD_DIST_SECCION",
    "COD_SECCION",
    "COD_EDAD_INT",
    "ESPANOLESHOMBRES",
    "ESPANOLESMUJERES",
    "EXTRANJEROSHOMBRES",
    "EXTRANJEROSMUJERES",
    "FX_CARGA",
    "FX_DATOS_INI",
    "FX_DATOS_FIN"
)
df_final = df_trimmed.fillna(0)
df_final.show()

df_barrios = df_final.select(
    "COD_BARRIO",
    "DESC_BARRIO",
).distinct().orderBy("DESC_BARRIO")
df_barrios.show(150)

df_final.createTempView("padron")

spark.sql("SELECT count(DISTINCT DESC_BARRIO) AS diferentes_barrios FROM padron").show()

df_final_len = df_final.withColumn("longitud", length("DESC_DISTRITO"))

df_final_len.show(1000)

df_final_no_len = df_final_len.drop("longitud")

df_final_no_len.show()

df_final_part = df_final_no_len.repartition("DESC_DISTRITO", "DESC_BARRIO")

df_final_part.show()

print(df_final_part.rdd.getNumPartitions())

df_cached = df_final_part.cache()

df_cached.show()

df_sums = df_cached.groupBy("DESC_DISTRITO", "DESC_BARRIO") \
    .agg(sum("ESPANOLESHOMBRES").alias("sum_esp_hom"), sum("ESPANOLESMUJERES").alias("sum_esp_muj"),
         sum("EXTRANJEROSHOMBRES").alias("sum_ext_hom"), sum("EXTRANJEROSMUJERES").alias("sum_ext_muj")) \
    .sort(desc("sum_ext_muj"), desc("sum_ext_hom"))

df_sums.show(30)

df_desc = df_final.groupBy("DESC_DISTRITO", "DESC_BARRIO") \
    .agg(sum("ESPANOLESHOMBRES").alias("sum_esp_hom")).orderBy("DESC_DISTRITO", "DESC_BARRIO")

df_desc.show()

df_joined = df_desc.join(df_final,
                         (df_final["DESC_BARRIO"] == df_desc["DESC_BARRIO"]) & (
                                 df_final["DESC_DISTRITO"] == df_desc["DESC_DISTRITO"]),
                         "inner")

df_joined.show()

windowSpec = Window.partitionBy("DESC_DISTRITO", "DESC_BARRIO").orderBy("DESC_DISTRITO", "DESC_BARRIO")

df_window = df_final.withColumn("sum_esp_hom", sum("ESPANOLESHOMBRES").over(windowSpec))

df_window.show()

df_contingencia = df_final \
    .filter((df_final["DESC_DISTRITO"] == "CENTRO") | (df_final["DESC_DISTRITO"] == "BARAJAS") | (
            df_final["DESC_DISTRITO"] == "RETIRO")) \
    .groupBy("COD_EDAD_INT") \
    .pivot("DESC_DISTRITO") \
    .agg(sum("ESPANOLESMUJERES").alias("sum_ext_muj")) \
    .orderBy("COD_EDAD_INT")

df_contingencia.show()

df_muj_edad_porc = df_contingencia\
    .withColumn("BARAJAS PERC", round(col("BARAJAS") / (col("CENTRO") + col("BARAJAS") + col("RETIRO")) * 100, 2))\
    .withColumn("CENTRO PERC", round(col("CENTRO") / (col("CENTRO") + col("BARAJAS") + col("RETIRO")) * 100, 2)) \
    .withColumn("RETIRO PERC", round(col("RETIRO") / (col("CENTRO") + col("BARAJAS") + col("RETIRO")) * 100, 2))

df_muj_edad_porc.show()

df_final.write.partitionBy("COD_DISTRITO", "COD_BARRIO").csv("padron-csv")

df_final.write.partitionBy("COD_DISTRITO", "COD_BARRIO").save("padron.parquet")

time.sleep(15)

df_cached.unpersist()

time.sleep(120)
