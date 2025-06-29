# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi Streaming Pipeline with Delta Live Tables

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

@dlt.table(
    comment="Raw NYC taxi trip data",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_taxi_trips():
    # Read from Databricks sample dataset
    # In real scenarios, this could be from Kafka, Event Hubs, etc.
    return (
        spark.readStream
            .format("delta")
            .option("maxFilesPerTrigger", 10)  # Process 10 files at a time
            .load("/databricks-datasets/nyctaxi/tripdata/yellow/")
            .select(
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime", 
                "trip_distance",
                "fare_amount",
                "pickup_zip",
                "dropoff_zip"
            )
    )

# COMMAND ----------

@dlt.table(
    comment="Cleaned and validated taxi trips",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_or_drop("valid_fare", "fare_amount > 0")
@dlt.expect_or_drop("valid_distance", "trip_distance > 0")
@dlt.expect_or_drop("valid_dates", "tpep_pickup_datetime < tpep_dropoff_datetime")
def silver_taxi_trips():
    return (
        dlt.read_stream("bronze_taxi_trips")
            .withColumn("trip_duration_minutes", 
                       (col("tpep_dropoff_datetime").cast("long") - 
                        col("tpep_pickup_datetime").cast("long")) / 60)
            .withColumn("pickup_date", to_date("tpep_pickup_datetime"))
            .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
    )

# COMMAND ----------

@dlt.table(
    comment="Hourly trip statistics",
    table_properties={
        "quality": "gold"
    }
)
def gold_hourly_stats():
    return (
        dlt.read_stream("silver_taxi_trips")
            .groupBy(
                window("tpep_pickup_datetime", "1 hour"),
                "pickup_zip"
            )
            .agg(
                count("*").alias("trip_count"),
                avg("fare_amount").alias("avg_fare"),
                avg("trip_distance").alias("avg_distance"),
                avg("trip_duration_minutes").alias("avg_duration_minutes")
            )
            .select(
                col("window.start").alias("hour_start"),
                "pickup_zip",
                "trip_count",
                round("avg_fare", 2).alias("avg_fare"),
                round("avg_distance", 2).alias("avg_distance"),
                round("avg_duration_minutes", 2).alias("avg_duration_minutes")
            )
    )

# COMMAND ----------

@dlt.table(
    comment="Data quality metrics",
    table_properties={
        "quality": "gold"
    }
)
def gold_data_quality_metrics():
    return (
        dlt.read_stream("bronze_taxi_trips")
            .groupBy(window("tpep_pickup_datetime", "1 day"))
            .agg(
                count("*").alias("total_records"),
                sum(when(col("fare_amount") <= 0, 1).otherwise(0)).alias("invalid_fares"),
                sum(when(col("trip_distance") <= 0, 1).otherwise(0)).alias("invalid_distances"),
                sum(when(col("tpep_pickup_datetime") >= col("tpep_dropoff_datetime"), 1)
                    .otherwise(0)).alias("invalid_dates")
            )
            .select(
                col("window.start").alias("date"),
                "total_records",
                "invalid_fares",
                "invalid_distances",
                "invalid_dates",
                round((col("invalid_fares") + col("invalid_distances") + col("invalid_dates")) 
                      / col("total_records") * 100, 2).alias("error_percentage")
            )
    )