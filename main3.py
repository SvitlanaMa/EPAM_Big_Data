# 5 most busy airports 01.06 - 31.08 in US (max total number of flights)

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

spark = SparkSession.builder.appName("air").getOrCreate()

df_2007 = spark.read.option("header", True).csv("2007.csv.bz2")
df_airp = spark.read.option("header", True).csv("airports.csv")

df = df_2007.filter( (df_2007.Year == "2007") & (df_2007.Month.isin(["6", "7", "8"])))
df1 = df.groupBy("Origin").count().select(sf.col("Origin"), sf.col("count").alias("Or_count"))
df2 = df.groupBy("Dest").count().select(sf.col("Dest"), sf.col("count").alias("De_count"))
df_res1 = df1.join(df2, df1.Origin == df2.Dest).select("Origin", sf.col("Or_count") + sf.col("De_count")).withColumnRenamed("(Or_count + De_count)", "Flights")
df_res1.join(df_airp, df_res1.Origin == df_airp.iata).select("iata", "airport", "Flights").filter(sf.col("country") == "USA").sort(sf.desc("Flights")).show(5)
