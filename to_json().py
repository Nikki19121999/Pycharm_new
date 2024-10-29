from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct

spark = SparkSession.builder.appName("to_json").getOrCreate()


data = [
    (1, "Nik", 29),
    (2, "Ram", 31),
    (3, "chey", 25)
]
df = spark.createDataFrame(data, ["Id", "Name", "Age"])
df.show()

df_json = df.select(to_json(struct("*")).alias("json"))
df_json.show()