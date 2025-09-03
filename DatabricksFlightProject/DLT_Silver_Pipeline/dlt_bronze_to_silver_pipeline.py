import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Check the data quality of the bronze data for bookings
rules = {
  'booking_id': 'booking_id IS NOT NULL',
  'passenger_id': 'passenger_id IS NOT NULL',
  'flight_id': 'flight_id IS NOT NULL',
  'airport_id': 'airport_id IS NOT NULL'
}
@dlt.expect_all_or_drop(rules)
# Create a view for bookings 
@dlt.view(name="bookings_raw")

def bookings_raw():
  df = spark.readStream.format("delta") \
              .load("/Volumes/workspace/bronze/bronzevolume/bookings/data")
  
  df = df.withColumn("amount", col("amount").cast(DoubleType()))\
        .withColumn("modifiedDate",current_timestamp())\
        .withColumn("booking_date", to_date(col("booking_date")))\
        .drop("_rescued_data")
  return df

# Create a streaming table for silver bookings
dlt.create_streaming_table(name="bookings_silver", comment="This is a transformed booking data")

# Create a CDC flow for silver bookings
dlt.create_auto_cdc_flow(
    source="bookings_raw",
    target="bookings_silver",
    keys = ["booking_id", "passenger_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)

# Create a view for flights
@dlt.view(
  name="flights_raw"
  )

def flights_raw():

  df = spark.readStream.format("delta") \
              .load("/Volumes/workspace/bronze/bronzevolume/flights/data")
  
  df = df.withColumn('flight_date', to_date(col("flight_date")))\
        .withColumn('modifiedDate', current_timestamp())\
        .drop("_rescued_data")
  return df

# Create a streaming table for silver flights
dlt.create_streaming_table(name="flights_silver", comment="This is a transformed flights data")

# Create a CDC flow for silver flights
dlt.create_auto_cdc_flow(
    name="flights_silver",
    source="flights_raw",
    target="flights_silver",
    keys = ["flight_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)

# Create a view for customers
@dlt.view(
  name="customers_raw"
)
def customers_raw():

  df = spark.readStream.format("delta") \
              .load("/Volumes/workspace/bronze/bronzevolume/customers/data")
  
  df = df.withColumn('modifiedDate', current_timestamp())\
          .drop("_rescued_data")
  return df

# Create a streaming table for silver customers
dlt.create_streaming_table(name="customers_silver", comment="This is a transformed customers data")

# Create a CDC flow for silver customers
dlt.create_auto_cdc_flow(
    name="customers_silver",
    source="customers_raw",
    target="customers_silver",
    keys = ["passenger_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)

# Create a view for airports
@dlt.view(
  name="airports_raw"
  )

def airports_raw():

  df = spark.readStream.format("delta") \
              .load("/Volumes/workspace/bronze/bronzevolume/airports/data")
  
  df = df.withColumn('modifiedDate', current_timestamp())\
          .drop("_rescued_data")
  return df

# Create a streaming table for silver airports
dlt.create_streaming_table(name="airports_silver", comment="This is a transformed airports data")

# Create a CDC flow for silver airports
dlt.create_auto_cdc_flow(
    name="airports_silver",
    source="airports_raw",
    target="airports_silver",
    keys = ["airport_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)


###########
@dlt.view(
  name="business_silver"
)

def business_silver():
  
  df = dlt.readStream("bookings_silver")\
                .join(dlt.readStream("flights_silver"), ["flight_id"])\
                .join(dlt.readStream("customers_silver"), ["passenger_id"])\
                .join(dlt.readStream("airports_silver"), ["airport_id"])\
                .drop("modifiedDate")
  
  return df