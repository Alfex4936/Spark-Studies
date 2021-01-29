from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs


def loadMovieNames():
    movieNames = {}
    with codecs.open(
        "./ml-100k/u.ITEM", "r", encoding="ISO-8859-1", errors="ignore"
    ) as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    return movieNames


# Spark v3.0.1
spark = SparkSession.builder.master("local").appName("PopularMovie").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

nameDict = spark.sparkContext.broadcast(loadMovieNames())

schema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

moviesDF = spark.read.option("sep", "\t").schema(schema).csv("./ml-100k/u.data")
lookupNameUDF = F.udf(lambda movieId: nameDict.value[movieId])

movieCounts = moviesDF.groupBy("movieID").count()
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(F.col("movieID")))

sortedMWN = moviesWithNames.orderBy(F.desc("count"))
sortedMWN.show(10, False)

spark.stop()
