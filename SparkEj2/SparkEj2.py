from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first

spark = (SparkSession
         .builder
         .appName("SparkEj1")
         .getOrCreate())

csv_udfs = "udfs.csv"

df = (spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .load(csv_udfs))

df.show(120)

df_M_CCY = (df.select(col("udf_name"), col("string_value"))
            .filter(col("udf_name") == "M_CCY"))

df_M_CCY.show()

df_pivoted = df.groupBy("nb", "udf_name").pivot("udf_name").agg(first("string_value"))

df_dropped_col = (df_pivoted.drop("M_DISCMARGIN").drop("M_DIRECTIAV")
                  .drop("M_LIQDTYCHRG").drop("M_CRDTCHRG")
                  .drop("M_MVA").drop("M_RVA").drop("M_PRUEBA"))

df_pivoted_num = df.groupBy("nb", "udf_name").pivot("udf_name").agg(first("num_value"))

df_pivoted_num.show()

df_dropped_col_num = (df_pivoted_num.drop("M_CLIENT").drop("M_SELLER")
                      .drop("M_CCY").drop("M_PRUEBA")
                      .drop("M_SUCURSAL").drop("nb"))

df_dropped_col_num.show()

df_joined = df_dropped_col.join(df_dropped_col_num, ["udf_name"])

df_joined.show(150)

filter_values = ["NULL", "null", "^\\s+$", "0.000000000000"]

columns_to_filter = ["M_DISCMARGIN", "M_DIRECTIAV", "M_LIQDTYCHRG", "M_CRDTCHRG", "M_MVA", "M_RVA", "M_SELLER"]

# df_filtered = df_joined.rdd.map(lambda c: ~col[c].isin(filter_values)).reduce(lambda x: x or x)
filtering_condition = reduce(lambda x, y: x | y, list(map(lambda x: ~col(x).isin(filter_values), columns_to_filter)))

df_filtered = df_joined.filter(filtering_condition)

df_filtered.show()
df_filtered.printSchema()

df_filtered2 = df_filtered.drop("udf_name").drop("M_CLIENT").drop("M_CCY").drop("M_SUCURSAL")

df_filtered2.show()

"""
df_test = df_filtered2.filter(((col("M_DISCMARGIN") == "NULL") | (col("M_DISCMARGIN") == "null")) &
                              ((col("M_DIRECTIAV") == "NULL") | (col("M_DIRECTIAV") == "null")) &
                              ((col("M_LIQDTYCHRG") == "NULL") | (col("M_LIQDTYCHRG") == "null")) &
                              ((col("M_CRDTCHRG") == "NULL") | (col("M_CRDTCHRG") == "null")) &
                              ((col("M_MVA") == "NULL") | (col("M_MVA") == "null")) &
                              ((col("M_RVA") == "NULL") | (col("M_RVA") == "null")) &
                              ((col("M_SELLER") == "NULL") | (col("M_SELLER") == "null")))

df_test.show()
"""
