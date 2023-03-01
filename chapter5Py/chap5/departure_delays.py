from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, when

# Create a SparkSession
spark = (SparkSession
         .builder
         .appName("SparkSQLExampleApp")
         .config("mysql-connector-j-8.0.31.jar")
         .getOrCreate())

jdbcDF = (spark
          .read
          .format("jdbc")
          .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("dbtable", "[TABLENAME]")
          .option("user", "[USERNAME]")
          .option("password", "[PASSWORD]")
          .load())

(jdbcDF
 .write
 .format("jdbc")
 .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
 .option("driver", "com.mysql.jdbc.Driver")
 .option("dbtable", "[TABLENAME]")
 .option("user", "[USERNAME]")
 .option("password", "[PASSWORD]")
 .save())


