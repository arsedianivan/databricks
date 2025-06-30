# Guide to Building a Streaming Pipeline with Delta Live Tables in Databricks (Python)

## What You'll Build
In this guide, you'll create a real-time data pipeline that:
- Reads NYC taxi trip data as it arrives
- Cleans and transforms the data
- Creates multiple tables showing different views of the data
- Updates automatically when new data arrives

## Important: Two Notebooks Required!
⚠️ **You'll create TWO separate notebooks:**
1. **DLT Pipeline Notebook** - Contains ONLY DLT table definitions (no display(), no regular queries!)
2. **Analysis Notebook** - For exploring data with queries and visualizations

## Prerequisites
- Access to a Databricks workspace
- Basic understanding of Python (we'll use simple functions)
- No advanced coding experience required!

## Step 1: Understanding the Components

**What is Delta Live Tables (DLT)?**
- Think of DLT as a smart assistant that helps you build data pipelines
- It automatically handles errors, retries, and data quality
- You write simple Python code, and DLT manages everything else

**What is Streaming?**
- Instead of processing all data at once, streaming processes data as it arrives
- Like watching a live TV show vs. a recorded one

## Step 2: Access the Sample Data

1. **Navigate to Data**
   - In your Databricks workspace, click on "Data" in the left sidebar
   - Click on "Add Data" → "Sample datasets"
   - Look for "NYC Taxi Trips" dataset

2. **Understanding the Data Path**
   - The NYC taxi data is typically stored at: `/databricks-datasets/nyctaxi/tripdata/yellow/`
   - Files are in csv.gz format (compressed CSV files)
   - Each file contains taxi trip records with pickup/dropoff times, locations, fares, etc.

## Step 3: Create Your First DLT Pipeline

1. **Navigate to Workflows**
   - Click "Workflows" in the left sidebar
   - Click "Delta Live Tables" tab
   - Click "Create Pipeline"

2. **Configure Your Pipeline**
   - **Pipeline name**: `nyc_taxi_streaming_pipeline`
   - **Product edition**: Choose "Core" (cheapest option for learning)
   - **Pipeline mode**: Select "Triggered" for now (easier to control)
   - **Target schema**: Type `taxi_analytics` (this is where your tables will be stored)

## Step 4: Write Your Pipeline Code

1. **Create a New Notebook**
   - Click "Create" → "Notebook"
   - Name it: `NYC_Taxi_DLT_Pipeline`
   - Select "Python" as the language

2. **Important**: This notebook should ONLY contain DLT table definitions. No display() commands, print statements, or regular queries!

3. **Add the Following Code** (copy and paste each section into separate cells):

### Cell 1: Import Required Libraries
```python
# Import DLT and PySpark functions
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
```

### Cell 2: Create the Bronze (Raw) Table
```python
@dlt.table(
    name="taxi_trips_bronze",
    comment="Raw NYC taxi trip data from CSV files"
)
def taxi_trips_bronze():
    # Read streaming data from CSV files
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-*.csv.gz")
        .withColumn("ingestion_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )
```

**What this does:**
- `@dlt.table`: Creates a DLT table
- `spark.readStream`: Reads data as a stream
- `cloudFiles`: Monitors folder for new files
- `inferColumnTypes`: Automatically detects data types
- We add timestamp and source file tracking

### Cell 3: Create the Silver (Cleaned) Table
```python
@dlt.table(
    name="taxi_trips_silver",
    comment="Cleaned taxi trips with data quality rules"
)
@dlt.expect_or_drop("valid_fare", "fare_amount >= 0")
@dlt.expect_or_drop("valid_trip_distance", "trip_distance > 0")
def taxi_trips_silver():
    # Read from bronze table and clean the data
    df = dlt.readStream("taxi_trips_bronze")
    
    return (
        df.select(
            col("tpep_pickup_datetime").alias("pickup_time"),
            col("tpep_dropoff_datetime").alias("dropoff_time"),
            "passenger_count",
            "trip_distance", 
            "fare_amount",
            "tip_amount",
            "total_amount",
            "payment_type",
            when(col("payment_type") == 1, "Credit card")
            .when(col("payment_type") == 2, "Cash")
            .when(col("payment_type") == 3, "No charge")
            .when(col("payment_type") == 4, "Dispute")
            .otherwise("Unknown").alias("payment_method"),
            to_date("tpep_pickup_datetime").alias("trip_date")
        )
        .filter(year("tpep_pickup_datetime") == 2019)
    )
```

**What this does:**
- `@dlt.expect_or_drop`: Drops rows that don't meet quality rules
- Renames columns to friendlier names
- Converts payment codes to readable text
- Filters to 2019 data only

### Cell 4: Create Gold (Summary) Tables
```python
# Daily summary statistics
@dlt.table(
    name="daily_taxi_summary",
    comment="Daily aggregated metrics for taxi trips"
)
def daily_taxi_summary():
    return (
        dlt.read("taxi_trips_silver")
        .groupBy("trip_date")
        .agg(
            count("*").alias("total_trips"),
            sum("fare_amount").alias("total_fares"),
            avg("trip_distance").alias("avg_distance"),
            avg("tip_amount").alias("avg_tip"),
            max("tip_amount").alias("max_tip"),
            countDistinct("payment_method").alias("payment_methods_used")
        )
    )

# Payment analysis table
@dlt.table(
    name="payment_analysis",
    comment="Analysis of payment methods and tipping patterns"
)
def payment_analysis():
    df = dlt.read("taxi_trips_silver")
    
    # Calculate tip percentage
    df_with_tip_pct = df.withColumn(
        "tip_percentage",
        when(col("total_amount") > 0, (col("tip_amount") / col("total_amount") * 100))
        .otherwise(0)
    )
    
    return (
        df_with_tip_pct
        .groupBy("payment_method")
        .agg(
            count("*").alias("trip_count"),
            avg("total_amount").alias("avg_total_fare"),
            avg("tip_amount").alias("avg_tip"),
            avg("tip_percentage").alias("avg_tip_percentage"),
            sum("total_amount").alias("total_revenue")
        )
    )

# Hourly patterns table
@dlt.table(
    name="hourly_patterns",
    comment="Taxi trip patterns by hour of day"
)
def hourly_patterns():
    df = dlt.read("taxi_trips_silver")
    
    return (
        df.withColumn("pickup_hour", hour("pickup_time"))
        .withColumn(
            "rush_hour_flag",
            when((hour("pickup_time").between(6, 9)) | (hour("pickup_time").between(17, 19)), 1)
            .otherwise(0)
        )
        .groupBy("pickup_hour")
        .agg(
            count("*").alias("trip_count"),
            avg("trip_distance").alias("avg_distance"),
            avg("total_amount").alias("avg_fare"),
            avg("rush_hour_flag").alias("rush_hour_percentage")
        )
        .orderBy("pickup_hour")
    )
```

**That's it for the DLT Pipeline notebook!** Only include the code above. Save the notebook now and move to Step 5.

## Step 5: Attach and Run Your Pipeline

1. **Attach the Notebook to Your Pipeline**
   - Go back to your pipeline settings
   - Under "Source code", click "Browse"
   - Select your `NYC_Taxi_DLT_Pipeline` notebook
   - Click "Save"

2. **Run the Pipeline**
   - Click "Start" to run your pipeline
   - Watch the progress in the pipeline graph view
   - Each table will show as a box, with arrows showing data flow

## Step 6: Explore Your Results

1. **View the Pipeline Graph**
   - You'll see a visual representation of your data flow
   - Bronze → Silver → Gold tables
   - Click on any table to see details

2. **Query Your Tables (IMPORTANT: Create a NEW Notebook!)**
   - Create a **new, separate notebook** (not your DLT pipeline notebook!)
   - Name it something like `NYC_Taxi_Analysis`
   - This is where you run queries to explore your data:
   
   ```python
   # See daily trends
   df_daily = spark.table("taxi_analytics.daily_taxi_summary")
   display(df_daily.orderBy("trip_date", ascending=False).limit(10))
   
   # Find highest tips
   df_tips = spark.table("taxi_analytics.taxi_trips_silver")
   display(df_tips.orderBy("tip_amount", ascending=False).limit(10))
   
   # Analyze payment methods
   df_payments = spark.table("taxi_analytics.payment_analysis")
   display(df_payments.orderBy("trip_count", ascending=False))
   
   # Visualize hourly patterns
   df_hourly = spark.table("taxi_analytics.hourly_patterns")
   display(df_hourly.orderBy("pickup_hour"))
   ```

**Remember**: 
- DLT Pipeline Notebook = Only DLT functions with @dlt decorators
- Analysis Notebook = Queries, display(), and data exploration

## Step 7: Making it Truly Streaming

To simulate real-time streaming:

1. **Change Pipeline Mode**
   - Edit your pipeline settings
   - Change "Pipeline mode" from "Triggered" to "Continuous"
   - This makes the pipeline run constantly, checking for new files

2. **Add New Data** (Advanced)
   - In production, new taxi files would arrive automatically
   - The pipeline would process them immediately
   - All downstream tables update automatically

## Troubleshooting Common Issues

**"UNRESOLVED_COLUMN" errors:**
- Different years of NYC taxi data have different column names
- Check available columns in a separate notebook:
  ```python
  df = spark.table("taxi_analytics.taxi_trips_bronze")
  df.printSchema()
  ```
- Common column variations:
  - Location IDs: `PULocationID`/`DOLocationID` vs `pickup_longitude`/`pickup_latitude`
  - Datetime: `tpep_pickup_datetime` vs `pickup_datetime`
  - Adjust your Silver and Gold table code to match your data's actual columns

**"AttributeError: 'module' object has no attribute 'readStream'":**
- Use `dlt.readStream()` for streaming sources in DLT, not `spark.readStream`
- Use `dlt.read()` for reading from other DLT tables

**"DLT pipeline cannot contain display() or print()":**
- Your DLT pipeline notebook can ONLY contain DLT function definitions
- Remove any display(), print(), or show() commands
- Put exploratory code in a separate notebook

**Unity Catalog Errors:**
- Use `col("_metadata.file_path")` instead of `input_file_name()`
- Ensure your target schema is in Unity Catalog format: `catalog.schema.table`

**Pipeline fails to start:**
- Check that your notebook is attached correctly
- Ensure you have permissions to create schemas
- Verify all imports are at the top of the notebook

**No data showing up:**
- Verify the file path is correct
- Check if the sample data exists in your workspace
- Look at the pipeline's error messages

**Out of memory errors:**
- Reduce the date range in your filter
- Process fewer files at once

## Next Steps

1. **Add More Transformations**
   ```python
   # Add distance categories
   .withColumn("distance_category",
       when(col("trip_distance") < 1, "Short")
       .when(col("trip_distance") < 5, "Medium")
       .otherwise("Long")
   )
   ```

2. **Add Data Quality Checks**
   ```python
   @dlt.expect("future_date_check", "pickup_time <= current_timestamp()")
   @dlt.expect_or_fail("reasonable_fare", "fare_amount < 1000")
   ```

3. **Create More Complex Aggregations**
   ```python
   # Time-based windows
   .groupBy(window("pickup_time", "1 hour"), "payment_method")
   ```

## Key Concepts to Remember

- **Bronze**: Raw data, exactly as it arrives
- **Silver**: Cleaned and standardized data
- **Gold**: Business-ready summaries and aggregations
- **@dlt.table**: Decorator that creates a DLT table
- **@dlt.expect**: Data quality rules
- **dlt.readStream()**: Read streaming data from DLT tables
- **dlt.read()**: Read batch data from DLT tables

## Useful Python Patterns for DLT

```python
# Define a streaming table
@dlt.table
def my_streaming_table():
    return spark.readStream.format("cloudFiles")...

# Add multiple expectations
@dlt.expect_or_drop("rule1", "column1 > 0")
@dlt.expect_or_drop("rule2", "column2 IS NOT NULL")
def my_clean_table():
    return dlt.readStream("source_table")...

# Create views (virtual tables)
@dlt.view
def my_view():
    return dlt.read("source_table").filter(...)

# Incremental processing (advanced)
@dlt.table
@dlt.incremental(
    source="source_table",
    keys=["id"],
    sequence_by="updated_at"
)
def my_incremental_table():
    return dlt.readStream("source_table")...
```

Congratulations! You've built your first streaming pipeline with Delta Live Tables using Python. The pipeline will automatically handle new data, maintain data quality, and keep your analytics tables up to date.