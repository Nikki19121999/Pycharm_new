from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Create DataFrame Example") \
    .getOrCreate()

# Sample data
data = [
    (1, "Alice", 29),
    (2, "Bob", 31),
    (3, "Cathy", 25)
]

# Define the schema
columns = ["id", "name", "age"]

# Create a DataFrame
df = spark.createDataFrame(data, schema=columns)

# Show the DataFrame
df.show()

# Stop the Spark session
spark.stop()
