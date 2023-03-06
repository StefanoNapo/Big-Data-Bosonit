import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, count_distinct, count, length, sum, desc


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

df_chached = df_final_part.cache()

df_chached.show()

df_sums = df_chached.groupBy("DESC_DISTRITO", "DESC_BARRIO")\
    .agg(sum("ESPANOLESHOMBRES").alias("sum_esp_hom"), sum("ESPANOLESMUJERES").alias("sum_esp_muj"),
         sum("EXTRANJEROSHOMBRES").alias("sum_ext_hom"), sum("EXTRANJEROSMUJERES").alias("sum_ext_muj"))\
    .sort(desc("sum_ext_muj"), desc("sum_ext_hom"))

df_sums.show(100)

time.sleep(30)

df_chached.unpersist()

time.sleep(120)

