from pyspark.sql import SparkSession
# creating the spark session
spark = SparkSession.builder.appName("Create DataFrame Example").getOrCreate()
#data
data = [
    (1, "Mahesh", 29),
    (2, "Rajesh", 31),
    (3, "Suresh", 25)
]
# defining the schema
columns = ["id", "name", "age"]

df = spark.createDataFrame(data, schema=columns)

df.show() # display the Dataframe
spark.stop() # Stop the sparksession
