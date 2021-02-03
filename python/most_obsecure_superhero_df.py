from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# Spark v3.0.1
spark = SparkSession.builder.master("local").appName("MostObsecureHero").getOrCreate()
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

minConnectionCount = connections.agg(F.min("connections")).first()[0]
minConnections = connections.filter(F.col("connections") == minConnectionCount)
minConnectionsWithNames = minConnections.join(names, "id")

print(f"Following characters only show up {minConnectionCount} time(s)")
minConnectionsWithNames.select("name").show()

spark.stop()
