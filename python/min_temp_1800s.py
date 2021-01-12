from pyspark.sql import SparkSession


def parseLine(line):
    stationId = line[0]  # Geolocation
    entryType = line[2]  # TMAX, TMIN
    temperature = int(line[3]) * 0.1  # -150 means -15.0 Celcius
    return (stationId, entryType, temperature)


# Spark v3.0.1
spark = SparkSession.builder.master("local").appName("MinTemp").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

lines = spark.read.option("header", False).csv("./data/1800_weather.csv")
weathers = lines.rdd.map(
    parseLine
)  # line is already list (Row= c0="ONE", c1="TWO", c2="33")
minTemps = weathers.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))  # (key, value) = (id, temperature)
minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))

results = minTemps.collect()

for result in results:
    print(f"{result[0]}\t{result[1]:.2f}")

spark.stop()
