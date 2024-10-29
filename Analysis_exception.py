# from pyspark.sql import SparkSession
# from pyspark.sql.utils import AnalysisException
#
# spark = SparkSession.builder.appName("Analysis_exception").getOrCreate()
# data = [("Nik", 1), ("ram", 2)]
# df = spark.createDataFrame(data, ["Name", "Id"])
#
# try:
#     df.select("NonExistentColumn").show()
# except AnalysisException as e:
#     print(f"Analysis Exception: {e}")

# Analysis Exception: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `NonExistentColumn` cannot be resolved.
# Did you mean one of the following? [`Name`, `Id`].;
# 'Project ['NonExistentColumn]
# +- LogicalRDD [Name#0, Id#1L], false

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.appName("Exception Example").getOrCreate()
# try:
#     df = spark.read.json("path/to/malformed_file.json")
# except AnalysisException as e:
#     print(f"Parse Exception: {e}")

try:
    df = spark.sql("SELECT * FROM xyz")
    df.show()
except AnalysisException as e:
    print(f"Query Execution Exception: {e}")
