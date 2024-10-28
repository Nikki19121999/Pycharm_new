from pyspark.sql.functions import posexplode, col
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pos_explode").getOrCreate()
data = [
    (1, 'nikhil', ['maths', 'eng'], [90, 80]),
    (2, 'mahesh', ['maths', 'eng'], [50, 90]),
    (3, 'balaji', ['maths', 'eng'], [96, 60])
]
schema = ('id', 'Name', 'sub', 'marks')
df = spark.createDataFrame(data, schema)
exploded_df = df.select("id", "Name", posexplode(col("sub")).alias("pos", "subject"))
result_df = exploded_df.select(
    "id",
    "Name",
    "subject",
    col("marks")[col("pos")].alias("marks")
)
result_df.show()
#posexplode_outer: This function is used to explode the sub array while also retaining the position index of each element.
# If sub is null or empty, it will still create a row with null values for the exploded columns.
