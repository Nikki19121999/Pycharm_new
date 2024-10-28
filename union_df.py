from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("union").getOrCreate()

data = [(101,"Mahesh","IT","ATP",100),(102,"Rakesh","HR","KNK",95),(103,"Harish","TT","BLR",88)]
schema= ["id","student_name","department_name","city","marks"]
df1 = spark.createDataFrame(data,schema)
data = [(108,"Rajesh","IT","ATP"),(106,"Ram","nk","kmo"),(104,"satish","ii","china")]
schema= ["id","student_name","department_name","city"]
df2 = spark.createDataFrame(data,schema)


df2= df2.withColumn("Marks",lit("null"))
df= df1.union(df2).show()
df.show()