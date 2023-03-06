from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim

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
