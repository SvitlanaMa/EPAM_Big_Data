# carrier with biggest number of flights

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

spark = SparkSession.builder.appName("air").getOrCreate()

df_2007 = spark.read.option("header", True).csv("2007.csv.bz2")
# df_carr = spark.read.option("header", True).csv("carriers.csv")

df_res1 = df_2007.groupBy("UniqueCarrier").count()
df_res1.sort(sf.desc("count")).withColumnRenamed("count", "Flights").show(1)
# max_fl = df_res1.agg({"count": "max"}).collect()[0][0]
# print(max_fl)
# df_res1.printSchema()
# df_res1.select("UniqueCarrier", "count").where("count" == sf.max("count")).show()
# df_res1.join(df_carr, df_res1.UniqueCarrier == df_carr.Code).select("Code", "Description", "count")
# df_res1.sort(sf.desc("count")).show(1)
# df_res1.show()

# add join + select or use for .. in ..
# df_res1.select(sf.first("UniqueCarrier"), sf.max("count")).show()