from __future__ import print_function

import sys

from pyspark.sql import SparkSession

from pyspark.sql import functions as func

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession
             .builder
             .appName("PythonMnMCount")
             .getOrCreate())
    # get the M&M data set file name
    mnm_file = sys.argv[1]
    # read the file into a Spark DataFrame
    mnm_df = (spark.read.format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(mnm_file)
              .repartition(10))
    mnm_df.show(n=5, truncate=False)

    # aggregate count of all colors and groupBy state and color
    # orderBy descending order
    count_mnm_df = (mnm_df.select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .sum("Count")
                    .orderBy("sum(Count)", ascending=False))

    # show all the resulting aggregation for all the dates and colors
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # find the aggregate count for California by filtering
    ca_count_mnm_df = (mnm_df.select("*")
                       .where(mnm_df.State == 'CA')
                       .groupBy("State", "Color")
                       .sum("Count")
                       .orderBy("sum(Count)", ascending=False))

    # show the resulting aggregation for California
    ca_count_mnm_df.show(n=10, truncate=False)

    state_by_max_mnm_df = (mnm_df
                           .groupBy("State")
                           .max("Count")
                           .orderBy("State", ascending=False))

    state_by_max_mnm_df.show(n=10, truncate=False)

    tx_count_mnm_df = (mnm_df
                       .where((mnm_df.State == 'TX') | (mnm_df.State == 'NV'))
                       .groupBy("State", "Color")
                       .sum("Count")
                       .orderBy("sum(Count)"))

    # show the resulting aggregation for California
    tx_count_mnm_df.show(n=10, truncate=False)

    co_operations_mnm_df = (mnm_df
                            .groupBy("State")
                            .agg(func.min("Count"),
                                 func.max("Count"),
                                 func.avg("Count"),
                                 func.count("Count"))
                            .orderBy(func.max("Count")))

    # show the resulting aggregation for California
    co_operations_mnm_df.show(n=10, truncate=False)

    mnm_df.createTempView("mnmTable")

    spark.sql("SELECT State, MAX(Count) FROM mnmTable GROUP BY State ORDER BY State DESC").show(n=10, truncate=False)

    spark.sql("SELECT State, Color, SUM(Count) FROM mnmTable WHERE State = 'TX' OR State = 'NV' GROUP BY State, " +
              "Color ORDER BY SUM(Count) DESC").show()

    spark.sql("SELECT State, MAX(Count) AS max_count, MIN(Count) AS min_count, AVG(Count) AS avg_count, COUNT(Count) " +
              "AS quantity FROM mnmTable GROUP BY State ORDER BY max_count DESC").show()

    state_by_max_mnm_df = (mnm_df.select())

    # ii. ¿Cómo obtener el número de particiones de un DataFrame? R=1
    numb_part = mnm_df.rdd.getNumPartitions()
    print(numb_part)

    # iii. ¿Qué formas existen para modificar el número de particiones de un DataFrame?
    mnm_df = mnm_df.repartition(1)
    mnm_df.count()
    numb_part = mnm_df.rdd.getNumPartitions()
    print(numb_part)
    # Utilizando el mismo ejemplo utilizado en el capítulo para guardar en parquet y
    # guardar los datos en los formatos:
    # i. JSON
    jsonPath = "C:/writeFiles/mnm_datasetJSONPy"
    mnm_df.write.format("json").save(jsonPath)
    # ii. CSV (dándole otro nombre para evitar sobrescribir el fichero origen)
    csvPath = "C:/writeFiles/mnm_datasetCSVPy"
    mnm_df.write.format("csv").save(csvPath)
    # iii. AVRO
    avroPath = "C:/writeFiles/mnm_datasetAVROPy"
    mnm_df.write.format("avro").save(avroPath)
