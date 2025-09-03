# Databricks notebook source
# MAGIC %md
# MAGIC ## Creating the required directories

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create volume under the raw folder
# MAGIC CREATE VOLUME if not exists workspace.rawflightsdata.rawvolume

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating a raw volume

# COMMAND ----------

# Create a directory under the rawvolume
dbutils.fs.mkdirs("/Volumes/workspace/rawflightsdata/rawvolume/rawdata")

# COMMAND ----------

# Create seperate directories under the rawdata for the raw data

dbutils.fs.mkdirs("/Volumes/workspace/rawflightsdata/rawvolume/rawdata/flights")
dbutils.fs.mkdirs("/Volumes/workspace/rawflightsdata/rawvolume/rawdata/bookings")
dbutils.fs.mkdirs("/Volumes/workspace/rawflightsdata/rawvolume/rawdata/customers")
dbutils.fs.mkdirs("/Volumes/workspace/rawflightsdata/rawvolume/rawdata/airports")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating the Bronze, Silver and Gold Layers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating the Bronze, silver and Gold volumes once the schemas are created 
# MAGIC CREATE SCHEMA if not exists workspace.bronze;
# MAGIC CREATE SCHEMA if not exists workspace.silver;
# MAGIC CREATE SCHEMA if not exists workspace.gold;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating the Bronze, silver and Gold volumes once the schemas are created 
# MAGIC CREATE VOLUME if not exists workspace.bronze.bronzevolume;
# MAGIC CREATE VOLUME if not exists workspace.silver.silvervolume;
# MAGIC CREATE VOLUME if not exists workspace.gold.goldvolume;

# COMMAND ----------

# MAGIC %md
# MAGIC **Once data Ingestion is done verifying the data**

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from delta.`/Volumes/workspace/bronze/bronzevolume/bookings/data`

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from delta.`/Volumes/workspace/bronze/bronzevolume/flights/data`

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from delta.`/Volumes/workspace/bronze/bronzevolume/customers/data`

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from delta.`/Volumes/workspace/bronze/bronzevolume/airports/data`