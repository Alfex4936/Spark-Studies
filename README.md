<div align="center">
<p>
    <img width="160" src="https://raw.githubusercontent.com/github/explore/6f5025830918df26b37d23b3ffffbc35725fe15f/topics/spark/spark.png">
</p>
<h1>Python Spark Studies</h1>
<h3>Spark v3.0.1 - Hadoop v3.2</h3>

[Apache Spark official website](https://spark.apache.org/)

</div>

# Installation

Download Spark from official webiste and extract them.

```console
export SPARK_HOME=/spark-path
export PATH=$PATH:$SPARK_HOME\bin

WIN10@DESKTOP:~$ cd $ENV:SPARK_HOME
WIN10@DESKTOP:SPARK_HOME$ pyspark
```

# Examples

## Ratings Counter

[python/ratings-counter.py](https://github.com/Alfex4936/Spark-Studies/blob/main/python/ratings-counter.py)

Basic example of how to read csv and map them.

1. Download movie rating datasets from [MovieLens (ml-latest-small)](https://grouplens.org/datasets/movielens/)

2. Run ratings-counter.py

```console
WIN10@DESKTOP:~$ spark-submit .\python\ratings-counter.py

or

WIN10@DESKTOP:~$ python .\python\ratings-counter.py
```