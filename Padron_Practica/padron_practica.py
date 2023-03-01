from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, when

spark = (SparkSession
         .builder
         .appName("padron_practica")
         .getOrCreate())

spark.sql("CREATE DATABASE datos_padron")

spark.sql("CREATE TABLE IF NOT EXISTS student_copy (COD_DISTRITO INT, DESC_DISTRITO STRING,COD_DIST_BARRIO INT, " +
          "DESC_BARRIO STRING, COD_BARRIO INT, COD_DIST_SECCION INT, COD_SECCION INT, COD_EDAD_INT INT, " +
          "ESPANOLESHOMBRES INT, ESPANOLESMUJERES INT, EXTRANJEROSHOMBRES INT,EXTRANJEROSMUJERES INT,FX_CARGA DATE, " +
          "FX_DATOS_INI DATE, FX_DATOS_FIN DATE) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' " +
          "DELIMITED FIELDS TERMINATED BY ';' WITH SERDEPROPERTIES ('quoteChar' = '\"') TBLPROPERTIES "
          "('skip.header.line.count'='1') USING CSV AS SELECT * FROM student STORED AS TEXTFILE;")

