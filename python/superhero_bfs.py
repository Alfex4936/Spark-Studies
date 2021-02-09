from pyspark.sql import SparkSession

# Spark v3.0.1
spark = SparkSession.builder.master("local").appName("BFS").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

startCharacterID = 5306  # Spider man
targetCharacterID = 14  # Adam
INF = 9999
COLORS = ("WHITE", "BLACK", "GRAY")

hitCounter = spark.sparkContext.accumulator(0)


def convertToBFS(line):
    line = line.split()
    heroID = int(line[0])
    connections = []
    for connection in line[1:]:
        connections.append(int(connection))

    color = COLORS[0]
    distance = INF
    if heroID == startCharacterID:
        color = COLORS[2]
        distance = 0

    return (heroID, (connections, distance, color))


def bfs(node):
    characterID = node[0]
    connections, distance, color = node[1]

    result = []

    if color == COLORS[2]:
        color = COLORS[1]
        for conn in connections:
            newCharacterID = conn
            newDistance = distance + 1
            newColor = COLORS[2]
            if targetCharacterID == conn:
                hitCounter.add(1)

            newEntry = (newCharacterID, (list(), newDistance, newColor))
            result.append(newEntry)
    result.append((characterID, (connections, distance, color)))
    return result


def bfsReduce(data1, data2):
    edges1, distance1, color1 = data1
    edges2, distance2, color2 = data2

    distance = INF
    color = COLORS[0]
    edges = []

    if edges1:
        edges = edges1
    elif edges2:
        edges = edges2

    if distance1 < distance:
        distance = distance1
    if distance2 < distance:
        distance = distance2

    if color1 == COLORS[0] and (color2 == COLORS[2] or color2 == COLORS[1]):
        color = color2
    if color2 == COLORS[2] and color2 == COLORS[1]:
        color = color2

    return edges, distance, color


lines = spark.sparkContext.textFile("./data/Marvel-graph.txt")
rdd = lines.map(convertToBFS)  # line is already list

for i in range(10):
    print(f"Running BFS iteration #{i + 1}")
    mapped = rdd.flatMap(bfs)

    print(f"Processing {mapped.count()} values.")

    if hitCounter.value > 0:
        print(f"Hit the target character! from {hitCounter.value}")
        break

    rdd = mapped.reduceByKey(bfsReduce)
spark.stop()
