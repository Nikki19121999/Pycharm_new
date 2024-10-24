# SparkSession

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import date, datetime
spark = SparkSession.builder.appName("Create DataFrame reading csv file").getOrCreate()
#data
Data=[
    (1,'Mahesh', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2,'Rajesh', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3,'Suresh', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
    ]

# • Schema
schema='ID long,Name string,Login_date date,login_datetime timestamp'

# • How to create the dataframe
df = spark.createDataFrame(Data,schema)

# • How to add the columns to df

df_addcolumn = df.withColumn("Gender",lit("M") )

# • How to rename the columns in df
df_renamed = df_addcolumn.withColumnRenamed("Name","New_Name")
# • Show(), collect()
df_renamed.show()
# • printSchema()
df_renamed.printSchema()