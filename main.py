# total number of flights per carrier in 2007
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

spark = SparkSession.builder.appName("air").getOrCreate()

df_2007 = spark.read.option("header", True).csv("2007.csv.bz2")
df_carr = spark.read.option("header", True).csv("carriers.csv")
# df_airp = spark.read.option("header", True).csv("airports.csv")

# df_2007.printSchema()
# df_2007.show(5)
df_res1 = df_2007.where(df_2007.Year=="2007").groupBy("UniqueCarrier").count()
df_res2 = df_res1.join(df_carr, df_res1.UniqueCarrier == df_carr.Code).select("Code", sf.col("Description").alias("Carrier"), sf.col("count").alias("Total_flights"))
df_res2.show()
# print(df_res1.columns)
