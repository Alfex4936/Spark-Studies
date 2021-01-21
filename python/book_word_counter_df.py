from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Spark v3.0.1
spark = SparkSession.builder.master("local").appName("WordCounterDF").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.text("./data/Self-Employment_book.txt")

# Split using a regular expression
words = df.select(F.explode(F.split(df.value, "\\W+")).alias("word"))
words.filter(words.word != "")

# Count words by occurrences
wordsCounts = words.groupBy("word").count().sort("count")
wordsCounts.show(wordsCounts.count())

spark.stop()
