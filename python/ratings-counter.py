from pyspark.sql import SparkSession
from collections import OrderedDict

# Spark v3.0.1
spark = SparkSession.builder.master("local").appName("RatingsHistogram").getOrCreate()

lines = spark.read.option("header", True).csv("./ml-latest-small/ratings.csv")
ratings = lines.rdd.map(lambda line: line[2])  # line is already list
result = ratings.countByValue()

sortedResult = OrderedDict(sorted(result.items()))
for key, value in sortedResult.items():
    print(f"{key}, {value}")

spark.stop()
