from pyspark.sql import SparkSession


def parseLine(line):
    fields = line.split(",")
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


# Spark v3.0.1
spark = SparkSession.builder.master("local").appName("RatingsHistogram").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

lines = spark.sparkContext.textFile("./data/fake_person.txt")
rdd = lines.map(parseLine)  # line is already list
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(
    lambda x, y: (x[0] + y[0], x[1] + y[1])
)
averageByAge = totalsByAge.mapValues(lambda x: x[0] // x[1])

results = averageByAge.collect()
for result in sorted(results, key=lambda x: x[0]):
    print(result)  # age, average friends number

spark.stop()
