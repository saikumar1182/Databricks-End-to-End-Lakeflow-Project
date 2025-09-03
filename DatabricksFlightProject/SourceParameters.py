# Databricks notebook source
# MAGIC %md
# MAGIC ### Creating Parameters for Job 

# COMMAND ----------

source_array = [
            {'source': 'bookings'},
            {'source': 'flights'},
            {'source': 'customers'},
            {'source': 'airports'}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key="output_key", value=source_array)