# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi Streaming Pipeline with Delta Live Tables
# MAGIC 
# MAGIC This pipeline processes NYC taxi trip data through three layers:
# MAGIC - **Bronze**: Raw data ingestion
# MAGIC - **Silver**: Cleaned and validated data
# MAGIC - **Gold**: Business-ready analytics

# Import required libraries
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

@dlt.table(
    comment="Raw NYC taxi trip data from compressed CSV files",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true"  # Allows pipeline reset during development
    }
)
def bronze_taxi_trips():
    """
    Reads streaming data from NYC taxi CSV files.
    Handles compressed .csv.gz files automatically.
    """
    return (
        spark.readStream
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("maxFilesPerTrigger", 1)  # Process 1 file at a time
            .load("/databricks-datasets/nyctaxi/tripdata/yellow/*.csv.gz")
            .select(
                # Select core columns that exist in the dataset
                col("tpep_pickup_datetime"),
                col("tpep_dropoff_datetime"), 
                col("trip_distance"),
                col("fare_amount"),
                # Add any other columns you found in Step 1
                col("passenger_count"),
                col("payment_type")
            )
            # Add placeholder columns for any missing data
            .withColumn("pickup_zip", lit("10001"))  # Manhattan zip as default
            .withColumn("dropoff_zip", lit("10001"))
    )

# COMMAND ----------

@dlt.table(
    comment="Cleaned and validated taxi trips with calculated fields",
    table_properties={
        "quality": "silver",
        "pipelines.reset.allowed": "true"
    }
)
@dlt.expect_or_drop("valid_fare", "fare_amount > 0 AND fare_amount < 1000")
@dlt.expect_or_drop("valid_distance", "trip_distance > 0 AND trip_distance < 100")
@dlt.expect_or_drop("valid_passenger_count", "passenger_count > 0 AND passenger_count <= 6")
@dlt.expect_or_drop("valid_datetime", "tpep_pickup_datetime < tpep_dropoff_datetime")
def silver_taxi_trips():
    """
    Cleans data by:
    - Removing invalid fares, distances, and passenger counts
    - Ensuring pickup happens before dropoff
    - Adding calculated fields for analysis
    """
    return (
        dlt.read_stream("bronze_taxi_trips")
            # Calculate trip duration in minutes
            .withColumn("trip_duration_minutes", 
                       (unix_timestamp("tpep_dropoff_datetime") - 
                        unix_timestamp("tpep_pickup_datetime")) / 60)
            # Extract date parts for easier analysis
            .withColumn("pickup_date", to_date("tpep_pickup_datetime"))
            .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
            .withColumn("pickup_day_of_week", dayofweek("tpep_pickup_datetime"))
            .withColumn("pickup_month", month("tpep_pickup_datetime"))
            # Add fare per mile
            .withColumn("fare_per_mile", 
                       when(col("trip_distance") > 0, col("fare_amount") / col("trip_distance"))
                       .otherwise(0))
    )

# COMMAND ----------

@dlt.table(
    comment="Hourly trip statistics by pickup location",
    table_properties={
        "quality": "gold",
        "pipelines.reset.allowed": "true"
    }
)
def gold_hourly_stats():
    """
    Aggregates trips by hour and pickup location.
    Useful for demand analysis and surge pricing.
    """
    return (
        dlt.read_stream("silver_taxi_trips")
            .withWatermark("tpep_pickup_datetime", "1 hour")
            .groupBy(
                window("tpep_pickup_datetime", "1 hour"),
                "pickup_zip"
            )
            .agg(
                count("*").alias("trip_count"),
                avg("fare_amount").alias("avg_fare"),
                avg("trip_distance").alias("avg_distance"),
                avg("trip_duration_minutes").alias("avg_duration"),
                avg("fare_per_mile").alias("avg_fare_per_mile"),
                sum("fare_amount").alias("total_revenue")
            )
            .select(
                col("window.start").alias("hour_start"),
                col("window.end").alias("hour_end"),
                "pickup_zip",
                "trip_count",
                round("avg_fare", 2).alias("avg_fare"),
                round("avg_distance", 2).alias("avg_distance"),
                round("avg_duration", 2).alias("avg_duration_minutes"),
                round("avg_fare_per_mile", 2).alias("avg_fare_per_mile"),
                round("total_revenue", 2).alias("total_revenue")
            )
    )

# COMMAND ----------

@dlt.table(
    comment="Daily trip summaries with key metrics",
    table_properties={
        "quality": "gold",
        "pipelines.reset.allowed": "true"
    }
)
def gold_daily_summary():
    """
    Daily rollup of key business metrics.
    Perfect for executive dashboards.
    """
    return (
        dlt.read_stream("silver_taxi_trips")
            .withWatermark("tpep_pickup_datetime", "1 day")
            .groupBy("pickup_date")
            .agg(
                count("*").alias("total_trips"),
                sum("fare_amount").alias("total_revenue"),
                avg("fare_amount").alias("avg_fare"),
                avg("trip_distance").alias("avg_distance"),
                avg("trip_duration_minutes").alias("avg_duration"),
                countDistinct("pickup_zip").alias("unique_pickup_locations"),
                sum("passenger_count").alias("total_passengers")
            )
            .select(
                "pickup_date",
                "total_trips",
                round("total_revenue", 2).alias("total_revenue"),
                round("avg_fare", 2).alias("avg_fare"),
                round("avg_distance", 2).alias("avg_distance_miles"),
                round("avg_duration", 2).alias("avg_duration_minutes"),
                "unique_pickup_locations",
                "total_passengers",
                round(col("total_revenue") / col("total_trips"), 2).alias("revenue_per_trip")
            )
    )

# COMMAND ----------

@dlt.table(
    comment="Track data quality metrics over time",
    table_properties={
        "quality": "gold",
        "pipelines.reset.allowed": "true"
    }
)
def gold_data_quality_metrics():
    """
    Monitors the health of our data pipeline.
    Tracks how many records fail validation.
    """
    bronze_stream = dlt.read_stream("bronze_taxi_trips")
    
    return (
        bronze_stream
            .withWatermark("tpep_pickup_datetime", "1 hour")
            .groupBy(window("tpep_pickup_datetime", "1 hour"))
            .agg(
                count("*").alias("total_records"),
                # Count various data quality issues
                sum(when(col("fare_amount") <= 0, 1).otherwise(0)).alias("invalid_fares"),
                sum(when(col("fare_amount") > 1000, 1).otherwise(0)).alias("excessive_fares"),
                sum(when(col("trip_distance") <= 0, 1).otherwise(0)).alias("invalid_distances"),
                sum(when(col("passenger_count") <= 0, 1).otherwise(0)).alias("invalid_passengers"),
                sum(when(col("tpep_pickup_datetime") >= col("tpep_dropoff_datetime"), 1)
                    .otherwise(0)).alias("invalid_times")
            )
            .select(
                col("window.start").alias("hour"),
                "total_records",
                "invalid_fares",
                "excessive_fares",
                "invalid_distances",
                "invalid_passengers",
                "invalid_times",
                # Calculate overall data quality score
                round(
                    ((col("total_records") - col("invalid_fares") - col("excessive_fares") - 
                      col("invalid_distances") - col("invalid_passengers") - col("invalid_times")) 
                     / col("total_records") * 100), 
                    2
                ).alias("quality_score_percent")
            )
    )