# carrier with biggest number of flights

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

spark = SparkSession.builder.appName("air").getOrCreate()

df_2007 = spark.read.option("header", True).csv("2007.csv.bz2")

df_res1 = df_2007.groupBy("UniqueCarrier").count()
df_res1.sort(sf.desc("count")).withColumnRenamed("count", "Flights").show(1)
