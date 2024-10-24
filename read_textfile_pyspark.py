from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("WordCountExample") \
    .getOrCreate()

# Read a text file
text_file = spark.read.text("C:\\Users\\korrakutinikhilkumar\\Downloads\\sample1.txt")

# Split lines into words
words = text_file.selectExpr("explode(split(value, ' ')) as word")

# Count occurrences of each word
word_counts = words.groupBy("word").count()

# Show the results
word_counts.show()

# Stop the Spark session
spark.stop()
