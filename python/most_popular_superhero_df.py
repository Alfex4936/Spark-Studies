from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# Spark v3.0.1
spark = SparkSession.builder.master("local").appName("MostPopularHero").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


schema = StructType(
    [StructField("id", IntegerType(), True), StructField("name", StringType(), True),]
)

names = spark.read.option("sep", " ").schema(schema).csv("./data/Marvel-names.txt")
lines = spark.read.text("./data/Marvel-graph.txt")

connections = (
    lines.withColumn("id", F.split(F.col("value"), " ")[0])
    .withColumn("connections", F.size(F.split(F.col("value"), " ")) - 1)
    .groupBy("id")
    .agg(F.sum("connections").alias("connections"))
)

mostPopular = connections.sort(F.col("connections").desc()).first()
mostPopularName = names.filter(F.col("id") == mostPopular[0]).select("name").first()

print(
    f"{mostPopularName[0]} is the most popular superhero with {mostPopular[1]} co-apperance."
)
spark.stop()
