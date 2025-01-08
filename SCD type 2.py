from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SCD").getOrCreate()

data = [
    (1, "active", "2023-01-01","2023-01-02", 1),
    (2, "hold", "2023-01-01", None, 1),
    (3, "non-active", "2023-01-01", None, 1)
]
columns = ["product_id", "status", "start_date", "end_date", "is_current"]
current_df = spark.createDataFrame(data, columns)
current_df.show()

current_df.