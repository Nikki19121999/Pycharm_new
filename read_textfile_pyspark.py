from pyspark.sql import SparkSession
from pyspark.sql.functions import col,desc
#creating a spark session
spark = SparkSession.builder.appName("WordCountExample").getOrCreate()
# reading the text file from the local
text_file = spark.read.text("C:\\Users\\korrakutinikhilkumar\\Downloads\\sample1.txt")
# splitting the sentence into words by using space
words = text_file.selectExpr("explode(split(value, ' ')) as word") #using the explode function to convert list of words into rows

word_counts = words.groupBy("word").count() #grouping by the word and counting the words
word_counts_sorted = word_counts.orderBy(col("count").desc()) #descending order
word_counts_sorted.show() # Display the dataframe
rdd = word_counts_sorted.rdd #converting df to rdd
print(rdd.collect()) #printing the rdd
spark.stop()

