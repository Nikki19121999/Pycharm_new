from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Create DataFrame Example").getOrCreate()

data = [
    (1, "Mahesh", 29),
    (2, "Rajesh", 31),
    (3, "Suresh", 25)
]
columns = ["id", "name", "age"]
df = spark.createDataFrame(data, schema=columns)
df.show()
spark.stop()
