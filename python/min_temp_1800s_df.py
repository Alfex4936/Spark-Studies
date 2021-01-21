from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)


# Spark v3.0.1
spark = SparkSession.builder.master("local").appName("MinTemp").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType(
    [
        StructField("stationID", StringType(), True),  # True for allowing no value
        StructField("date", IntegerType(), True),
        StructField("measure_type", StringType(), True),
        StructField("temperature", FloatType(), True),
    ]
)

df = spark.read.schema(schema).csv("./data/1800_weather.csv")
df.printSchema()

# Filter out all but TMIN
minTemps = df.filter(df.measure_type == "TMIN")

# Select only id, temp
stationTemps = minTemps.select("stationID", "temperature")

# Aggregate(집합) to find min temp for every station
minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation.show()

minTempsByStationF = minTempsByStation.withColumn(
                    "temperature", F.round(F.col("min(temperature)") * 0.1, 2)
                )\
                .select("stationID", "temperature")\
                .sort("temperature")

# Collect, format and print the results
results = minTempsByStationF.collect()

for result in results:
    print(f"{result[0]}\t{result[1]:.2f}")

spark.stop()
