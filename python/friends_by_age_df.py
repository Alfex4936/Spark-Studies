from pyspark.sql import SparkSession
from pyspark.sql import functions as func


def parseLine(line):
    fields = line.split(",")
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


# Spark v3.0.1
spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

people = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("./data/fakefriends-header.csv")
)

friendsByAge = people.select("age", "friends")
friendsByAge.groupBy("age").avg("friends").show()

friendsByAge.groupBy("age").avg("friends").sort("age").show()

friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# Change column name round(avg(friends), 2) to friends_avg
friendsByAge.groupBy("age").agg(
    func.round(func.avg("friends"), 2).alias("friends_avg")
).sort("age").show()

spark.stop()
