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

# You should use unionByName() in the following scenarios:
#
# Different column orders: When the DataFrames have the same columns but in different orders.
# Missing columns: When the DataFrames have some columns missing, and you want to fill those missing columns with null values.
# Renamed columns: When the columns have different names but represent the same data, and you want to align them before performing the union.

#unionall() -it's not available in pyspark
# where as in the sql it will give with duplicates.

#  union() in spark will give all values even duplicates also.but in sql it will give distinct values.

