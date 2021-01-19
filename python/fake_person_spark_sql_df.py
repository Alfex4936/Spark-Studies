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

people = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("./data/fakefriends-header.csv")
)

print("Inferred schema:")
people.printSchema()

print("Name column:")
people.select("name").show()

print("Filter age < 21:")
people.filter(people.age < 21).show()

print("Group by age:")
people.groupBy("age").count().show()

print("Make everyone + 10y older:")
people.select(people.name, people.age + 10).show()
