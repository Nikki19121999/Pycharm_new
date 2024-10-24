from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Create DataFrame Example").getOrCreate()

data = [
    (1, "Alice", 29),
    (2, "Bob", 31),
    (3, "Cathy", 25)
]
columns = ["id", "name", "age"]
df = spark.createDataFrame(data, schema=columns)
df.show()
spark.stop()
