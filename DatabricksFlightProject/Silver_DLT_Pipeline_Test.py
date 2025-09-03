# Databricks notebook source
# MAGIC %md
# MAGIC ### Sample testing Notebook for verifying the data and trasformation for the ETL pipeline

# COMMAND ----------

df = spark.read.format("delta") \
                .load("/Volumes/workspace/bronze/bronzevolume/bookings/data")

display(df)

# COMMAND ----------

from pyspark.sql.functions import col, lit, cast, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

df = df.withColumn("amount", col("amount").cast(DoubleType()))\
        .withColumn("modifiedDate",current_timestamp())\
        .withColumn("booking_date", to_date(col("booking_date")))\
        .drop("_rescued_data")

display(df)

# COMMAND ----------

df = spark.read.format("delta") \
                .load("/Volumes/workspace/bronze/bronzevolume/flights/data")

display(df)


# COMMAND ----------

from pyspark.sql.functions import col, lit, cast, current_timestamp, to_date
df = df.withColumn('flight_date', to_date(col("flight_date")))\
        .withColumn('modifiedDate', current_timestamp())\
        .drop("_rescued_data")
display(df)

# COMMAND ----------

df = spark.read.format("delta") \
                .load("/Volumes/workspace/bronze/bronzevolume/customers/data")

display(df)

# COMMAND ----------

df = df.withColumn('modifiedDate', current_timestamp())\
        .drop("_rescued_data")
display(df)

# COMMAND ----------

df = spark.read.format("delta") \
                .load("/Volumes/workspace/bronze/bronzevolume/airports/data")
display(df)


# COMMAND ----------

df = df.withColumn('modifiedDate', current_timestamp())\
        .drop("_rescued_data")
display(df)

# COMMAND ----------

import dlt
@dlt.view(
  name="stage_bookings"
  )

def stage_bookings():

  df = spark.readStream.format("delta") \
              .load("/Volumes/workspace/bronze/bronzevolume/bookings/data")
  
  df = df.withColumn("amount", col("amount").cast(DoubleType()))\
        .withColumn("modifiedDate",current_timestamp())\
        .withColumn("booking_date", to_date(col("booking_date")))\
        .drop("_rescued_data")
  return df

dlt.create_streaming_table(name="silver_bookings", comment="This is a transformed booking data")

dlt.create_auto_cdc_flow(
    name="silver_bookings",
    source="stage_bookings",
    target="silver_bookings",
    keys = ["booking_id", "passenger_id"],
    sequence_by = col("booking_id"),
    stored_as_scd_type = 1
)

@dlt.view(
  name="stage_flights"
  )

def stage_flights():

  df = spark.readStream.format("delta") \
              .load("/Volumes/workspace/bronze/bronzevolume/flights/data")
  
  df = df.withColumn('flight_date', to_date(col("flight_date")))\
        .withColumn('modifiedDate', current_timestamp())\
        .drop("_rescued_data")
  return df

dlt.create_streaming_table(name="silver_flights", comment="This is a transformed flights data")
dlt.create_auto_cdc_flow(
    name="silver_flights",
    source="stage_flights",
    target="silver_flights",
    keys = ["flight_id"],
    sequence_by = col("flight_id"),
    stored_as_scd_type = 1
)

@dlt.view(
  name="stage_customers"
  )

def stage_customers():

  df = spark.readStream.format("delta") \
              .load("/Volumes/workspace/bronze/bronzevolume/customers/data")
  
  df = df.withColumn('modifiedDate', current_timestamp())\
          .drop("_rescued_data")
  return df

dlt.create_streaming_table(name="silver_customers", comment="This is a transformed customers data")
dlt.create_auto_cdc_flow(
    name="silver_customers",
    source="stage_customers",
    target="silver_customers",
    keys = ["customer_id"],
    sequence_by = col("customer_id"),
    stored_as_scd_type = 1
)

@dlt.view(
  name="stage_airports"
  )

def stage_airports():

  df = spark.readStream.format("delta") \
              .load("/Volumes/workspace/bronze/bronzevolume/airports/data")
  
  df = df.withColumn('modifiedDate', current_timestamp())\
          .drop("_rescued_data")
  return df

dlt.create_streaming_table(name="silver_airports", comment="This is a transformed airports data")
dlt.create_auto_cdc_flow(
    name="silver_airports",
    source="stage_airports",
    target="silver_airports",
    keys = ["airport_id"],
    sequence_by = col("airport_id"),
    stored_as_scd_type = 1
)




# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.silver_flights
# MAGIC

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS workspace.silver.silver_flights;
# DROP TABLE IF EXISTS workspace.silver.silver_airports;
# DROP TABLE IF EXISTS workspace.silver.silver_customers;
# DROP TABLE IF EXISTS workspace.silver.silver_business;
# DROP TABLE IF EXISTS workspace.silver.silver_bookings;