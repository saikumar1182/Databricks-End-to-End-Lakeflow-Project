# Databricks notebook source
# MAGIC %md
# MAGIC ### Incremenetal Data Ingestion For Flights Data

# COMMAND ----------

dbutils.widgets.text("source","")

# COMMAND ----------

source_value = dbutils.widgets.get('source')

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
          .option("cloudFiles.format", "csv") \
          .option("cloudFiles.schemaLocation", f"/Volumes/workspace/bronze/bronzevolume/{source_value}/checkpoint")\
          .option("cloudFiles.schemaEvolutionMode", "rescue") \
          .load(f"/Volumes/workspace/rawflightsdata/rawvolume/rawdata/{source_value}/")

# COMMAND ----------

df.writeStream.format("delta") \
            .trigger(once=True) \
            .outputMode("append")\
            .option("checkpointLocation", f"/Volumes/workspace/bronze/bronzevolume/{source_value}/checkpoint") \
            .option("path", f"/Volumes/workspace/bronze/bronzevolume/{source_value}/data") \
            .start()