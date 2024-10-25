from pyspark.sql import SparkSession
from datetime import date

from pyspark.sql.functions import years, upper, lower, ltrim, rtrim
from pyspark.sql.types import StructType,StructField,DateType,IntegerType,StringType #importing the sql types
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("Pyspark_functions").getOrCreate()

data = [
        (1,"Mahesh ",date(2000,5,12)),
        (2," Rajesh",date(1999,12,19)),
        (3,"Sur esh",date(2001,5,19))
]
#different ways of defining Schema
columns = ["id", "Name", "DOB"]
# schema = ("id" "int","Name" "string","DOB" "Date")
# using_schema = StructType([StructField("id",IntegerType(),True),
#                           StructField("Name",StringType(),True),
#                           StructField("Date_of_birth",DateType(),True)
#                           ])

df = spark.createDataFrame(data,columns)
row_count = df.count()
print(f"count: {row_count}")
df.select(F.col("Name")).show()
df.filter(F.col("Name")=="Suresh").show()
df.describe("Name").show()
Columns = df.columns
print(f'columns:{Columns}')
df.filter(F.col("Name").like("%jesh")).show()
df.withColumn("ninty's", F.when(F.year("DOB") < 2000,1).otherwise(0)).show()
df.select(F.col("Name").alias("Full_Name")).show()
df.orderBy(F.col("id").desc()).show()
df.groupBy(F.col("id")).count().show()
df.agg(F.max("id")).alias("max_id").show()
df.agg(F.min("id").alias("min_id")).show()
df.agg(F.count("id").alias("id_count")).show()
df.agg(F.avg("id").alias("id_average")).show()
df.agg(F.sum("id").alias("sum_of_id")).show()

# String Functions

df.select(upper("Name").alias("U_Names")).show()
df.select(lower("Name").alias("L_Names")).show()
df.select(ltrim("Name").alias("ltrim")).show()
df.select(rtrim("Name").alias("rtrim")).show()
df.select(F.trim("Name").alias("trim")).show()
df.select(F.rpad("Name",10,"_").alias("rpad")).show()
df.select(F.lpad("Name",10,"_").alias("lpad")).show()
df.select(F.length("Name").alias("Length_of_name")).show()
df.select(F.substring("Name",1,4).alias("substr")).show()

 #Subtstring_index
df.select("id",F.substring_index("Name"," ",1).alias("index")).show()

df.select(F.translate("Name","a","*").alias("trans_str")).show()

df.select(F.repeat("Name",2).alias("rep")).show()

df.select(F.regexp_replace("Name","esh","Nik").alias("str")).show()

df.select(F.regexp_extract("Name","esh",0).alias("ext")).show()



