from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("Word Count").getOrCreate()
data = [("hello",), ("world",), ("hello",), ("databricks",), ("hello",), ("world",)]
words = spark.createDataFrame(data, ["word"])
word_counts = words.groupBy("word").count()
word_counts_sorted = word_counts.orderBy(col("count").desc())
word_counts_sorted.show()
spark.stop()
