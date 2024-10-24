from pyspark.sql import SparkSession
from pyspark.sql.functions import col,desc
spark = SparkSession.builder \
    .appName("WordCountExample") \
    .getOrCreate()


text_file = spark.read.text("C:\\Users\\korrakutinikhilkumar\\Downloads\\sample1.txt")


words = text_file.selectExpr("explode(split(value, ' ')) as word")


word_counts = words.groupBy("word").count()
word_counts_sorted = word_counts.orderBy(col("count").desc())

word_counts_sorted.show()
rdd = word_counts_sorted.rdd
print(rdd.collect())
spark.stop()

