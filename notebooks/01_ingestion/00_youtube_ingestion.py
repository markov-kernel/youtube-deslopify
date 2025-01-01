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

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read and Process Channel Statistics

# COMMAND ----------

# Read the existing JSON file from the data volume
input_path = "/Volumes/yt-deslopify/default/youtube/channel_statistics.json"

# Read the JSON file into a DataFrame
df = spark.read.json(input_path)

# Select and rename columns as needed
df_final = df.select(
    df.channel.alias("name"),
    df.url,
    df.views
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create Unity Catalog Table

# COMMAND ----------

# Create the table in Unity Catalog
table_name = "yt-deslopify.default.channel_statistics"

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
# MAGIC SELECT * FROM yt-deslopify.default.channel_statistics LIMIT 5;
