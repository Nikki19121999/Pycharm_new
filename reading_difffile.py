from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
spark = SparkSession.builder.appName("Read_file").getOrCreate()
json_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

df_json = spark.read.schema(json_schema).json("C:/Users/KorrakutiNikhilKumar/Downloads/Json.json")
df_json.show()
df_csv = spark.read.csv("C:\\Users\\korrakutinikhilkumar\\Downloads\\annual-enterprise-survey-2023-financial-year-provisional.csv", header=True, inferSchema=True)
df_csv.show()


