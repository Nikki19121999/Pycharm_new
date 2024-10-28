from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col,split

spark = SparkSession.builder.appName("union").getOrCreate()
Data= [(101,"mathew",28,"30|38|20"),(102,"Steve",49,"29|20|30"),(104,"Rob",30,"40|38|29")]
schema= ["id","Name","Age","Marks"]
df= spark.createDataFrame(Data,schema)
df.show()

df1=df.withColumn("Mark",split(df.Marks,'\\|'))
df2=df1.withColumn("physics",col("Mark").getItem(0)).withColumn("Maths",col("Mark").getItem(1)).withColumn("Chemistry",col("Mark").getItem(2))
#In PySpark, if you want to perform an outer explosion, you typically use explode_outer instead of posexplode or explode.
# This will ensure that you retain rows even when the array is null or empty, resulting in null for the exploded fields in those cases.
df2.select("id","Name","Age","physics","Maths","Chemistry").show()
df3=df1.withColumn("Marks",explode(col("Mark"))).drop(col("Mark")).show()