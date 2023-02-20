# number of fl jun 2007 by NYC
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

spark = SparkSession.builder.appName("air").getOrCreate()

df_2007 = spark.read.option("header", True).csv("2007.csv.bz2")
# df_carr = spark.read.option("header", True).csv("carriers.csv")
df_airp = spark.read.option("header", True).csv("airports.csv")
# df_2007.join(df_airp, (df_airp.iata == df_2007.Origin) or (iata.iata == df_2007.Dest)).filter()
# df_2007.printSchema()
# df_2007.select("Origin", "Dest").show(5)
#  df_airp.where(df_airp.city == "New York").show()
# iata = df_airp.filter(df_airp.city.like("%New York%")).select("iata").collect()
iata = df_airp.filter(df_airp.city.like("%New York%")).select("iata").collect()
iata_list = [data[0] for data in iata]
# print(iata_list)
df_2007.filter( (df_2007.Year == "2007") & (df_2007.Month == "6") & (df_2007.DayofMonth == "1") & (df_2007.Origin.isin(iata_list) | df_2007.Dest.isin(iata_list)) ).select("Year", "Month", "DayofMonth", "DepTime", "FlightNum", "Origin", "Dest").show()
# flight_data.join(iata, (iata.iata == flight_data.Origin) or (iata.iata == flight_data.Dest))
# Origin or Dest == iata