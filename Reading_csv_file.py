from pyspark.sql import SparkSession
# creating the spark session
spark = SparkSession.builder.appName("Create DataFrame reading csv file").getOrCreate()
df = spark.read.csv("C:\\Users\\korrakutinikhilkumar\\Downloads\\annual-enterprise-survey-2023-financial-year-provisional.csv", header=True, inferSchema=True)

df.show(5)
df.printSchema()
# rdd = df.rdd
# print(rdd.collect())
