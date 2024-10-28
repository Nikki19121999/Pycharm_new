from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.appName("Transform").getOrCreate()

data = [
    (1, [1, 2, 3]),
    (2, [4, 5]),
    (3, None),
    (4, [7, 8, 9])
]
df = spark.createDataFrame(data, ["id", "numbers"])
transformed_df = df.select("id",expr("transform(numbers, x -> x + 1) as incremented_numbers"))
transformed_df.show()
