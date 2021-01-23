from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    FloatType,
)


schema = StructType(
    [
        StructField("customerID", IntegerType(), True),  # True for allowing no value
        StructField("itemID", IntegerType(), True),
        StructField("cost", FloatType(), True),
    ]
)


# Spark v3.0.1
spark = SparkSession.builder.master("local").appName("MoneySpent").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Parse with pre-defined schema
df = spark.read.option("header", True).schema(schema).csv("./data/customer_orders.csv")
df.printSchema()

infos = df.select("customerID", "cost")

infos = (
    infos.groupBy("customerID")
    .agg(F.round(F.sum("cost"), 2).alias("cost_spent"))
    .sort("cost_spent")
)

infos.show(infos.count())  # default showing rows = 20, so count how many rows in df and print them all

spark.stop()
