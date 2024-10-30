from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Pivot").getOrCreate()
l = [
    (1, "Gaga", "India", "2022-01-11"),
    (1, "Katy", "UK", "2022-01-11"),
    (1, "Bey", "Europe", "2022-01-11"),
    (2, "Gaga", None, "2022-10-11"),
    (2, "Katy", "India", "2022-10-11"),
    (2, "Bey", "US", "2022-02-15"),
    (3, "Gaga", "Europe", "2022-10-11"),
    (3, "Katy", "US", "2022-10-11"),
    (3, "Bey", None, "2022-02-15"),
    (1, "Gaga", "US", "2022-01-11"),
    (3, "Katy", "Switz", "2022-02-15"),
]
df = spark.createDataFrame(l, ["ID", "NAME", "COUNTRY", "Date_part"])
df1 = df.groupBy("ID", "Date_part").pivot("NAME").agg(collect_list("COUNTRY"))
df1.show()