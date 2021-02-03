from pyspark.sql import Row, SparkSession


def parseLine(line):
    return Row(
        id=int(line[0]),
        name=str(line[1].encode("utf-8")),
        age=int(line[2]),
        numFriends=int(line[3]),
    )


# Spark v3.0.1
spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

lines = spark.read.option("header", True).csv("./data/fakefriends-header.csv")
people = lines.rdd.map(parseLine)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")  # db name

# Option 1
teens = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
for teen in teens.collect():
    print(teen.name, teen.age, teen.numFriends)

# Option 2
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
