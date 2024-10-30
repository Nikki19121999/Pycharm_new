from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col

spark = SparkSession.builder.appName("From_json").getOrCreate()

data=[
    ('John Doe','{"street": "123 Main St", "city": "Anytown"}'),
    ('Jane Smith','{"street": "456 Elm St", "city": "Othertown"}')
]

df=spark.createDataFrame(data,schema="name string,address string")

df= df.withColumn("Parsed_json",from_json(col("address"),'street string,city string'))

df_final=df.select(col("name"),col("Parsed_json").street.alias("street"),col("Parsed_json").city.alias("city"))

df_final.show()
