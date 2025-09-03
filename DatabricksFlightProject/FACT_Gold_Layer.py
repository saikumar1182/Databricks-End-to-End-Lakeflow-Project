# Databricks notebook source
# MAGIC %md
# MAGIC ## Defining Varaiables for Fact Table

# COMMAND ----------

# Catalog Name
catalog = "workspace"

# CDC column
cdc_col = "modifiedDate"

# Backdated Refresh
backdated_refresh = ""

# Source Schema
source_schema = "silver"

# Source Object
source_object = "bookings_silver"

# Source fact table
fact_table = f"{catalog}.{source_schema}.{source_object}"

# Target Schema
target_schema = "gold"

# Target Object
target_object = "Fact_Bookings"

# COMMAND ----------

# Define Dimensions for tables and their respective keys

dimensions = [
    {
        "table" : f"{catalog}.{target_schema}.DimCustomers",
        "alias" : "DimCustomers",
        "join_keys" : [("passenger_id", "passenger_id")] # fact_col and dim_col
    },
    {
        "table" : f"{catalog}.{target_schema}.DimFlights",
        "alias" : "DimFlights",
        "join_keys" : [("flight_id", "flight_id")]  # fact_col and dim_col
    },
    {
        "table" : f"{catalog}.{target_schema}.DimAirports",
        "alias" : "DimAirports",
        "join_keys" : [("airport_id", "airport_id")]  # fact_col and dim_col
    },
]

# Fact Table Columns 
fact_columns = ["amount", "booking_date", "modifiedDate"]

# COMMAND ----------

# MAGIC %md
# MAGIC **Last Load Date**

# COMMAND ----------


# No back dated refresh
if len(backdated_refresh) == 0:

  # If table exist in the target
  if not(spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}")):

    last_load = spark.sql(f"SELECT MAX({cdc_col}) FROM {catalog}.{source_schema}.{source_object}").collect()[0][0]

  else:
    last_load = "1900-01-01 00:00:00"

# Back Dated Refresh
else:
  last_load = backdated_refresh


# COMMAND ----------

# Function to generate fact table query based on the dimensions

def generate_fact_table_incremental(fact_table, fact_columns, dimensions, cdc_col, processing_date):

    fact_alias = "f"

    # Base columns to select
    base_cols = [f"{fact_alias}.{col}" for col in fact_columns]

    # Build Joins dynamically
    join_conditions = []
    for dim in dimensions:
        full_table = dim["table"]
        dim_alias = dim["alias"]
        table_name = full_table.split(".")[-1]
        surrogate_key = f"{dim_alias}.{table_name}Key"
        base_cols.append(surrogate_key)

        on_condition = [f"{fact_alias}.{fk} = {dim_alias}.{dk}" for fk, dk in dim["join_keys"]]

        join_condition = f"LEFT JOIN {full_table} AS {dim_alias} ON " + " AND ".join(on_condition)
        join_conditions.append(join_condition)

    # Final Select and Join Condition
    select_clause = ",\n ".join(base_cols)
    joins = "\n".join(join_conditions)

    where_clause = f"{fact_alias}.{cdc_col} >= DATE('{last_load}')"


    #Final Query
    query = f"""
        SELECT
            {select_clause}
        FROM
            {fact_table} AS {fact_alias}
        {joins}
        WHERE
            {where_clause}
        """.strip()

    return query               
    

# COMMAND ----------

# Generate Fact Table Query
query = generate_fact_table_incremental(fact_table, fact_columns, dimensions, cdc_col, last_load)

# COMMAND ----------

# print(query)

# SELECT
#             f.amount,
#  f.booking_date,
#  f.modifiedDate,
#  DimCustomers.DimCustomersKey,
#  DimFlights.DimFlightsKey,
#  DimAirports.DimAirportsKey
#         FROM
#             workspace.silver.bookings_silver AS f
#         LEFT JOIN workspace.gold.DimCustomers AS DimCustomers ON f.passenger_id = DimCustomers.passenger_id
# LEFT JOIN workspace.gold.DimFlights AS DimFlights ON f.flight_id = DimFlights.flight_id
# LEFT JOIN workspace.gold.DimAirports AS DimAirports ON f.airport_id = DimAirports.airport_id
#         WHERE
#             f.modifiedDate >= DATE('1900-01-01 00:00:00')

# COMMAND ----------

# Read Fact Table

df_fact = spark.sql(query)

# df_fact.display()

# COMMAND ----------


# from pyspark.sql.functions import col
# df_fact.groupBy("DimCustomersKey", "DimFlightsKey", "DimAirportsKey").count().filter("count > 1").display()

# df_fact.filter((col('DimCustomersKey') == 190) & (col('DimFlightsKey') == 68) & (col('DimAirportsKey') == 27)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Upsert**

# COMMAND ----------

# Define Key columns for Fact Table
fact_key_cols = ["DimCustomersKey", "DimFlightsKey", "DimAirportsKey", 'booking_date']

fact_key_cols_str = " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])
fact_key_cols_str

# COMMAND ----------

# Merge fact table into Delta Lake
from delta.tables import DeltaTable

# Check if table exists in the target 
if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg")\
                  .merge(df_fact.alias("src"), fact_key_cols_str)\
                  .whenMatchedUpdateAll(condition=f"src.{cdc_col} >= trg.{cdc_col}")\
                  .whenNotMatchedInsertAll()\
                  .execute()

# Write and save as table
else:
    df_fact.write.format("delta")\
                  .mode("append")\
                  .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

# %sql
# select * from workspace.gold.fact_bookings

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(*) as count, DimAirportsKey from workspace.gold.dimairports group by DimAirportsKey having count(*) > 1;
# MAGIC -- select count(*) as count, DimFlightsKey from workspace.gold.dimflights group by DimFlightsKey having count(*) > 1;
# MAGIC -- select count(*) as count, DimCustomersKey from workspace.gold.dimcustomers group by DimCustomersKey having count(*) > 1

# COMMAND ----------

