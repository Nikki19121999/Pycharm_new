from Pyspark_dataframe import spark

df = spark.read.csv("C:\\Users\\korrakutinikhilkumar\\Downloads\\annual-enterprise-survey-2023-financial-year-provisional.csv", header=True, inferSchema=True)


rdd = df.rdd


print(rdd.collect())
