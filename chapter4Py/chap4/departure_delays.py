from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, when

# Create a SparkSession
spark = (SparkSession
         .builder
         .appName("SparkSQLExampleApp")
         .getOrCreate())
# Path to data set
csv_file = "departuredelays.csv"
# Read and create a temporary view
# Infer schema (note that for larger files you
# may want to specify the schema)
df = (spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csv_file))
df.createOrReplaceTempView("us_delay_flights_tbl")

spark.sql("SELECT distance, origin, destination FROM us_delay_flights_tbl " +
          "WHERE distance > 1000 ORDER BY distance DESC").show(10)

spark.sql("SELECT date, delay, origin, destination " +
          "FROM us_delay_flights_tbl " +
          "WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' " +
          "ORDER by delay DESC").show(10)

spark.sql(
    """SELECT delay, origin, destination,
   CASE
   WHEN delay > 360 THEN 'Very Long Delays'
   WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
   WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
   WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
   WHEN delay = 0 THEN 'No Delays'
   ELSE 'Early'
   END AS Flight_Delays
   FROM us_delay_flights_tbl
   ORDER BY origin, delay DESC""").show(10)

(df.select("distance", "origin", "destination")
 .where(col("distance") > 1000)
 .orderBy(desc("distance"))).show(10)

(df.select("date", "delay", "origin", "destination")
 .where((col("delay") > 120) | (col("origin") == 'SFO') | (col("destination") == 'ORD'))
 .orderBy(desc("delay"))).show(10)

(df.select(col("*"),
           when(df.delay > 360, "Very Long Delays")
           .when((df.delay > 120) & (df.delay < 360), "Long Delays")
           .when((df.delay > 60) & (df.delay < 120), "Short Delays")
           .when((df.delay > 0) & (df.delay < 60), "Tolerable Delays")
           .when(df.delay == 0, "No Delays")
           .otherwise("Early").alias("Flight_Delays"))
 .orderBy(df.origin.desc(), df.delay.desc())).show(10)

csv_file = "departuredelays.csv"
schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(csv_file, schema)
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

df.write.option("path", "/tmp/data/us_flights_delay").saveAsTable("us_delay_flights_tbl")

df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")

df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")

spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns("us_delay_flights_tbl")

us_flights_df = spark.sql("SELECT * FROM us_delay_flights_tbl")
us_flights_df2 = spark.table("us_delay_flights_tbl")

us_flights_df.show()
us_flights_df2.show()


file = "C:/writeFiles/mnm_datasetJSON/*"
df_mnm_json = spark.read.format("json").option("path", file).load()
df_mnm_json.show(10, False)

spark.sql("CREATE OR REPLACE TEMPORARY VIEW mnm_json USING json OPTIONS (path "
          "'C:/writeFiles/mnm_datasetJSON/*')")
spark.sql("SELECT * FROM mnm_json").show()


file = "C:/writeFiles/mnm_datasetCSV/*"
df_mnm_csv = spark.read.format("json").option("path", file).load()
df_mnm_csv.show(10, False)

spark.sql("CREATE OR REPLACE TEMPORARY VIEW mnm_csv USING csv OPTIONS (path "
          "'C:/writeFiles/mnm_datasetCSV/*')")
spark.sql("SELECT * FROM mnm_csv").show()