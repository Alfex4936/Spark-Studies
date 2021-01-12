from pyspark.sql import SparkSession


def parseLine(line):
    customerId = int(line[0])  # customer id
    itemId = line[1]  # item id
    moneySpent = float(line[2])  # dollar
    return (customerId, itemId, moneySpent)


# Spark v3.0.1
spark = SparkSession.builder.master("local").appName("MoneySpent").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

lines = spark.read.option("header", True).csv("./data/customer_orders.csv")
rdds = lines.rdd.map(parseLine)

infos = rdds.map(lambda x: (x[0], x[2]))  # (key, value) = (id, money)
totalSpent = infos.reduceByKey(lambda x, y: x + y)

# totalSpentFlip = totalSpent.map(lambda x, y: y, x)  # (key, value) = (value, key)
# results = totalSpentFlip.sortByKey()

results = totalSpent.collect()

for result in sorted(results, key=lambda x: x[0]):  # sort by customer id
    print(f"{result[0]}\t{result[1]:.2f}")

spark.stop()
