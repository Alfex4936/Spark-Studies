from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType


# Spark v3.0.1
spark = SparkSession.builder.master("local").appName("PopularMovie").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


schema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

moviesDF = spark.read.option("sep", "\t").schema(schema).csv("./ml-100k/u.data")
topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(F.desc("count"))
topMovieIDs.sort("movieID").show(10)

spark.stop()
