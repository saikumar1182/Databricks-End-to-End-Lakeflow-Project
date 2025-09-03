# Databricks notebook source
# MAGIC %md
# MAGIC ### Defining Variables for dimention tables

# COMMAND ----------

# Import Required Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Catalog Name
catalog = "workspace"

# Keycolumns
keycols = "['flight_id']"
key_cols_list = eval(keycols)

# CDC column
cdc_col = "modifiedDate"

# Backdated Refresh
backdated_refresh = ""

# Source Schema
source_schema = "silver"

# Source Object
source_object = "flights_silver"

# Target Schema
target_schema = "gold"

# Target Object
target_object = "DimFlights"

# Surrogate Key
surrogate_key = "DimFlightsKey"



# COMMAND ----------

# # Catalog Name
# catalog = "workspace"

# # Keycolumns
# keycols = "['passenger_id']"
# key_cols_list = eval(keycols)

# # CDC column
# cdc_col = "modifiedDate"

# # Backdated Refresh
# backdated_refresh = ""

# # Source Schema
# source_schema = "silver"

# # Source Object
# source_object = "customers_silver"

# # Target Schema
# target_schema = "gold"

# # Target Object
# target_object = "DimCustomers"

# # Surrogate Key
# surrogate_key = "DimCustomersKey"



# COMMAND ----------

# # Catalog Name
# catalog = "workspace"

# # Keycolumns
# keycols = "['airport_id']"
# key_cols_list = eval(keycols)

# # CDC column
# cdc_col = "modifiedDate"

# # Backdated Refresh
# backdated_refresh = ""

# # Source Schema
# source_schema = "silver"

# # Source Object
# source_object = "airports_silver"

# # Target Schema
# target_schema = "gold"

# # Target Object
# target_object = "DimAirports"

# # Surrogate Key
# surrogate_key = "DimAirportsKey"



# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Data Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC #### Last Load

# COMMAND ----------


# No back dated refresh
if len(backdated_refresh) == 0:

  # If table exist in the target
  if not(spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}")):

    last_load = spark.sql(f"SELECT MAX({cdc_col}) FROM {catalog}.{source_schema}.{source_object}").collect()[0][0]

  # If table does not exist in the target
  else:
    last_load = "1900-01-01 00:00:00"

# Backdated Refresh
else:
  last_load = backdated_refresh
  

# COMMAND ----------

# MAGIC %md
# MAGIC #### OLD Records VS NEW Records

# COMMAND ----------

# If table exist in the target
if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
  
  key_cols_string_incremental = ", ".join(key_cols_list)

  # Create a target table
  df_trg = spark.sql(f"SELECT {key_cols_string_incremental},{surrogate_key}, create_date, update_date FROM {catalog}.{target_schema}.{target_object}")

# If table does not exist in the target
else:
    key_cols_string_initial =[f"'' AS {x}" for x in key_cols_list]
    key_cols_string_initial = ", ".join(key_cols_string_initial)

    df_trg = spark.sql(f"""SELECT {key_cols_string_initial},CAST('0' AS INT) AS {surrogate_key}, CAST('1900-01-01 00:00:00' AS timestamp) AS create_date,
                       CAST('1900-01-01 00:00:00' AS timestamp) AS update_date WHERE 1=0""")


# COMMAND ----------

# MAGIC %md
# MAGIC **Join Condition**

# COMMAND ----------

# Create a source table

df_src = spark.sql(f"SELECT * FROM {catalog}.{source_schema}.{source_object} WHERE {cdc_col} >= '{last_load}'")

# COMMAND ----------

# Create a join table condition

join_condition = ' AND '.join([f"src.{x} = trg.{x}" for x in key_cols_list])

# COMMAND ----------

# Create temp views for both source and target

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")

# Create a join tables for old and new records
df_join = spark.sql(f"""
          select src.*,
                trg.{surrogate_key},
                trg.create_date,
                trg.update_date
            from src
            left join trg
            on {join_condition}
                    
        """)

# COMMAND ----------

# df_join.display()

# COMMAND ----------

# OLD Records
df_old = df_join.filter(col(f"{surrogate_key}").isNotNull())

# New Records
df_new = df_join.filter(col(f"{surrogate_key}").isNull())

# COMMAND ----------

# df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preparing Old Dataframe 

# COMMAND ----------

# Old dataframe enrichment
df_old_enriched = df_old.withColumn("update_date", current_timestamp())


# df_old_enriched.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preparing New Dataframe

# COMMAND ----------

# New dataframe enrichment and adding the surrogate key column and monotonic id

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    max_surrogate_key = spark.sql(f"SELECT MAX({surrogate_key}) FROM {catalog}.{target_schema}.{target_object}").collect()[0][0]

    df_new_enriched = df_new.withColumn(f"{surrogate_key}", lit(max_surrogate_key) + lit(1) + monotonically_increasing_id())\
                    .withColumn("create_date", current_timestamp())\
                    .withColumn("update_date", current_timestamp())

else:
    max_surrogate_key = 0
    df_new_enriched = df_new.withColumn(f"{surrogate_key}", lit(max_surrogate_key) + lit(1) + monotonically_increasing_id())\
                    .withColumn("create_date", current_timestamp())\
                    .withColumn("update_date", current_timestamp())


# COMMAND ----------

# df_new_enriched.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Union Old and New Records

# COMMAND ----------

# Union of both old and new

df_union = df_old_enriched.unionByName(df_new_enriched)

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPSERT

# COMMAND ----------

# Import Delta Table library
from delta.tables import DeltaTable

# Merge logic for Delta Table on the surrogate key
if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg")\
                  .merge(df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}")\
                  .whenMatchedUpdateAll(condition=f"src.{cdc_col} >= trg.{cdc_col}")\
                  .whenNotMatchedInsertAll()\
                  .execute()

# If table does not exist in the target
else:
    df_union.write.format("delta")\
                  .mode("append")\
                  .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS workspace.gold.dimairports
# MAGIC -- DROP TABLE IF EXISTS workspace.gold.dimflights
# MAGIC -- DROP TABLE IF EXISTS workspace.gold.dimcustomers
# MAGIC -- select * from workspace.gold.dimcustomers