# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # YouTube Channel Statistics Table Creation
# MAGIC This notebook reads channel statistics from a JSON file in the data volume and creates a table in Unity Catalog.
# MAGIC 
# MAGIC ## Table Details
# MAGIC - **Catalog**: yt-deslopify
# MAGIC - **Schema**: default
# MAGIC - **Table**: channel_statistics

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read and Process Channel Statistics

# COMMAND ----------

# Read the existing JSON file from the data volume
input_path = "/Volumes/yt-deslopify/default/youtube/channel_statistics.json"

# Read the JSON file into a DataFrame
df = spark.read.json(input_path, multiLine=True)

# Debug: Print schema and show sample data
print("DataFrame Schema:")
df.printSchema()
print("\nSample Data:")
df.show(5, truncate=False)

# Explode the top_50_channel_urls array and select needed columns
df_final = df.selectExpr("explode(top_50_channel_urls) as channel_info").select(
    col("channel_info.channel").alias("name"),
    col("channel_info.url"),
    col("channel_info.views")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create Unity Catalog Table

# COMMAND ----------

# Create the table in Unity Catalog
table_name = "`yt-deslopify`.default.channel_statistics"

# Create or replace the table
df_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"Successfully created table {table_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify Table Creation
# MAGIC Run the following command to verify the table was created successfully:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `yt-deslopify`.default.channel_statistics LIMIT 5;
