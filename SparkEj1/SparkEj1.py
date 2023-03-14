import time

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, to_timestamp, rank, desc

spark = (SparkSession
         .builder
         .appName("SparkEj1")
         .getOrCreate())

csv_trade = "trade_details.csv"

csv_trade_snap = "trade_details_snapshot.csv"

df = (spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .load(csv_trade))

df_snap = (spark.read.format("csv")
           .option("inferSchema", "true")
           .option("header", "true")
           .option("delimiter", ";")
           .load(csv_trade_snap))

df.show()

df_snap.show()

df_snap_filtered = (df_snap.select("origin_contract_number", col("maturity").alias("maturity2"))
                    .filter((col("mfamily") == "CURR") | (col("mgroup") == "FXD") | (col("mtype") == "SWLEG")))

df_fxswap = (df.select("mfamily", "mgroup", "mtype", "origin_trade_number", col("origin_contract_number"), to_timestamp("maturity").alias("maturity"))
             .filter((col("mfamily") == "CURR") | (col("mgroup") == "FXD") | (col("mtype") == "SWLEG"))
             .join(df_snap_filtered,
                   ["origin_contract_number"], "left")
             .orderBy(df["origin_contract_number"], col("maturity"), col("maturity2")))

df_fxswap.show(50)
df_fxswap.printSchema()
print(df_fxswap.count())

windowSpec = Window.partitionBy("origin_contract_number").orderBy(desc(col("maturity")), desc(col("maturity2")))

df_semi = (df_fxswap.withColumn("rank", rank().over(windowSpec)))

df_final = (df_semi.distinct()
            .filter(col("rank") == 1)
            .drop(col("rank"))
            .drop(col("maturity2")))

df_semi.show()

df_final.show()



