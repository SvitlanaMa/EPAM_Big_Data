# number of fl jun 2007 by NYC
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

spark = SparkSession.builder.appName("air").getOrCreate()

df_2007 = spark.read.option("header", True).csv("2007.csv.bz2")
df_airp = spark.read.option("header", True).csv("airports.csv")

iata = df_airp.filter(df_airp.city.like("%New York%")).select("iata").collect()
iata_list = [data[0] for data in iata]

df_2007.filter( (df_2007.Year == "2007") & (df_2007.Month == "6") & (df_2007.DayofMonth == "1") & (df_2007.Origin.isin(iata_list) | df_2007.Dest.isin(iata_list)) ).select("Year", "Month", "DayofMonth", "DepTime", "FlightNum", "Origin", "Dest").show()
