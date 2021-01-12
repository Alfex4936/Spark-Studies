import re as regex

from pyspark.sql import SparkSession


def normalizeWords(text):
    return regex.compile(r"\W+", regex.UNICODE).split(text)


# Spark v3.0.1
spark = SparkSession.builder.master("local").appName("WordCounter2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

lines = spark.sparkContext.textFile("./data/DaddyLongLeg_book_korean.txt")
words = lines.flatMap(normalizeWords)
wordCounts = words.countByValue()

# mostFreqWord = max(wordCounts, key=lambda x: wordCounts[x])
# print(f"{mostFreqWord}: {wordCounts[mostFreqWord]} times")

for word, count in sorted(wordCounts.items(), key=lambda x: x[1]):
    cleanWord = word.encode("utf-8", "ignore")
    if count > 10:
        print(cleanWord.decode("utf-8"), count)

spark.stop()
