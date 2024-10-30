from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

# Create Spark session
spark = SparkSession.builder \
    .appName("SCD Type 2 Example Without Union") \
    .getOrCreate()

# Sample DataFrames for current and new records
current_df = spark.createDataFrame([
    (1, "John Doe", "Team A", True),
    (2, "Jane Smith", "Team B", True)
], ["player_id", "name", "team", "is_current"])

new_records_df = spark.createDataFrame([
    (2, "Jane Smith", "Team C"),  # Update for Jane
    (3, "Alice Brown", "Team D")   # New record for Alice
], ["player_id", "name", "team"])

# Step 1: Identify records to update (Team changes for existing records)
updates_df = current_df.alias("current") \
    .join(new_records_df.alias("new"), "player_id", "inner") \
    .filter(col("current.team") != col("new.team")) \
    .select(
        col("current.player_id").alias("player_id"),
        col("current.name").alias("current_name"),
        col("current.team").alias("current_team"),
        lit(False).alias("is_current")  # Mark as historical
    )

# Step 2: Historical records DataFrame (for the historical part)
historical_df = updates_df.select(
    col("player_id"),
    col("current_name").alias("name"),
    col("current_team").alias("team"),
    col("is_current")
)

# Step 3: Prepare new current records DataFrame (for the new/current records)
new_current_df = new_records_df.alias("new") \
    .join(current_df.alias("current"), "player_id", "left") \
    .select(
        col("new.player_id"),
        col("new.name").alias("name"),
        col("new.team").alias("team"),
        when(col("current.player_id").isNotNull(), lit(True)).otherwise(lit(False)).alias("is_current")
    )

# Step 4: Combine current and historical records into one final DataFrame
final_df = current_df.alias("current") \
    .join(historical_df.alias("historical"), "player_id", "outer") \
    .join(new_current_df.alias("new_current"), "player_id", "outer") \
    .select(
        # Use qualified names for the columns to avoid ambiguity
        col("current.player_id").alias("player_id"),
        when(col("new_current.name").isNotNull(), col("new_current.name"))
            .otherwise(col("current.name")).alias("name"),
        when(col("new_current.team").isNotNull(), col("new_current.team"))
            .otherwise(col("current.team")).alias("team"),
        when(col("new_current.is_current") == True, lit(True))
            .otherwise(when(col("historical.is_current") == False, lit(False))).alias("is_current")
    ) \
    .dropDuplicates()

# Show the final DataFrame
final_df.show(truncate=False)
