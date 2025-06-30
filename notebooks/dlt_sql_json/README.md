# Beginner's Guide to Building an IoT Streaming Pipeline with Delta Live Tables in Databricks

## What You'll Build

In this guide, you'll create a real-time data pipeline that:

- Reads IoT fitness device data (steps, calories, miles walked) as it arrives
- Cleans and validates the data
- Creates summary tables showing user activity patterns
- Updates automatically when new data arrives

## Important: Two Notebooks Required!
### ⚠️ You'll create TWO separate notebooks:

- DLT Pipeline Notebook - Contains ONLY CREATE TABLE statements (no SELECT queries!)
- Analysis Notebook - For exploring data with SELECT queries

## Prerequisites
- Access to a Databricks workspace
- Basic understanding of SQL (we'll use simple queries)
- No coding experience required!

# Step 1: Understanding Your IoT Data
What's in the IoT Dataset? The sample IoT data simulates fitness trackers sending data about:

- **device_id**: Which fitness tracker sent the data
- **user_id**: Which person is wearing the tracker
- **num_steps**: How many steps they've taken
- **miles_walked**: Distance covered
- **calories_burnt**: Energy expended
- **timestamp**: When this activity happened

Think of it as getting real-time updates from thousands of Fitbits!

### Data Location

Path: > /databricks-datasets/iot-stream/data-device/
Format: JSON files (JavaScript Object Notation - a common data format)
Step 2: Create Your DLT Pipeline
Navigate to Workflows
Click "Workflows" in the left sidebar
Click "Delta Live Tables" tab
Click "Create Pipeline"
Configure Your Pipeline
Pipeline name: iot_fitness_streaming_pipeline
Product edition: Choose "Core" (most affordable for learning)
Pipeline mode: Select "Triggered" (easier to control while learning)
Target schema: Type fitness_analytics (where your tables will live)
Click "Save"
Step 3: Create Your Pipeline Notebook
Create a New Notebook
Click "Create" → "Notebook"
Name it: IoT_Fitness_DLT_Pipeline
Select "SQL" as the language
Important Reminder: This notebook should ONLY contain CREATE TABLE statements!
Add the Following Code (copy each section into separate cells):
Cell 1: Bronze Table (Raw Data)
sql
-- This reads the raw IoT device data as it arrives
CREATE OR REFRESH STREAMING LIVE TABLE fitness_devices_bronze
COMMENT "Raw IoT fitness device data from JSON files"
AS SELECT 
  *,
  current_timestamp() as ingestion_time,
  _metadata.file_path as source_file
FROM cloud_files(
  "/databricks-datasets/iot-stream/data-device/*.json",
  "json",
  map("cloudFiles.inferColumnTypes", "true")
);
What this does:

Monitors the folder for new JSON files
Reads each file as it arrives
Adds tracking info (when we processed it, which file it came from)
Stores everything in a "bronze" table (industry term for raw data)
Cell 2: Silver Table (Cleaned Data)
sql
-- This creates a cleaned, validated version of the data
CREATE OR REFRESH STREAMING LIVE TABLE fitness_devices_silver
(
  -- Data quality rules: drop bad records
  CONSTRAINT valid_steps EXPECT (num_steps >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_calories EXPECT (calories_burnt >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_miles EXPECT (miles_walked >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT reasonable_steps EXPECT (num_steps < 100000) ON VIOLATION DROP ROW
)
COMMENT "Cleaned fitness data with quality checks"
AS SELECT
  device_id,
  user_id,
  num_steps,
  miles_walked,
  calories_burnt,
  -- Convert timestamp string to proper timestamp
  to_timestamp(timestamp) as activity_timestamp,
  -- Extract useful time parts
  date(to_timestamp(timestamp)) as activity_date,
  hour(to_timestamp(timestamp)) as activity_hour,
  -- Calculate steps per mile (if miles > 0)
  CASE 
    WHEN miles_walked > 0 THEN num_steps / miles_walked 
    ELSE 0 
  END as steps_per_mile,
  -- Categorize activity level
  CASE
    WHEN num_steps < 1000 THEN 'Low'
    WHEN num_steps BETWEEN 1000 AND 5000 THEN 'Moderate'
    WHEN num_steps BETWEEN 5001 AND 10000 THEN 'Active'
    ELSE 'Very Active'
  END as activity_level
FROM STREAM(LIVE.fitness_devices_bronze)
WHERE device_id IS NOT NULL 
  AND user_id IS NOT NULL;
What this does:

Removes records with negative values (data errors)
Removes unrealistic step counts (over 100,000)
Converts the timestamp to a proper date/time format
Adds helpful calculated fields like activity level
Only keeps records with valid device and user IDs
Cell 3: Gold Tables (Business Analytics)
sql
-- Daily user summary
CREATE OR REFRESH LIVE TABLE daily_user_summary
COMMENT "Daily activity summary per user"
AS SELECT
  user_id,
  activity_date,
  COUNT(DISTINCT device_id) as devices_used,
  SUM(num_steps) as total_steps,
  SUM(miles_walked) as total_miles,
  SUM(calories_burnt) as total_calories,
  AVG(num_steps) as avg_steps_per_reading,
  MAX(num_steps) as max_steps_in_reading,
  -- Daily goal achievement (10,000 steps)
  CASE 
    WHEN SUM(num_steps) >= 10000 THEN 'Goal Achieved'
    ELSE 'Below Goal'
  END as daily_goal_status
FROM LIVE.fitness_devices_silver
GROUP BY user_id, activity_date;

-- Hourly activity patterns
CREATE OR REFRESH LIVE TABLE hourly_activity_patterns
COMMENT "Activity patterns throughout the day"
AS SELECT
  activity_hour,
  COUNT(DISTINCT user_id) as active_users,
  AVG(num_steps) as avg_steps,
  AVG(calories_burnt) as avg_calories,
  SUM(num_steps) as total_steps,
  -- Identify peak hours
  CASE
    WHEN activity_hour BETWEEN 6 AND 9 THEN 'Morning'
    WHEN activity_hour BETWEEN 12 AND 13 THEN 'Lunch'
    WHEN activity_hour BETWEEN 17 AND 19 THEN 'Evening'
    WHEN activity_hour BETWEEN 20 AND 22 THEN 'Night'
    ELSE 'Other'
  END as time_period
FROM LIVE.fitness_devices_silver
GROUP BY activity_hour
ORDER BY activity_hour;

-- User fitness leaderboard
CREATE OR REFRESH LIVE TABLE user_fitness_rankings
COMMENT "Overall user fitness rankings"
AS SELECT
  user_id,
  COUNT(DISTINCT activity_date) as active_days,
  COUNT(DISTINCT device_id) as devices_used,
  SUM(num_steps) as lifetime_steps,
  SUM(miles_walked) as lifetime_miles,
  SUM(calories_burnt) as lifetime_calories,
  AVG(num_steps) as avg_daily_steps,
  -- Rank users by total steps
  RANK() OVER (ORDER BY SUM(num_steps) DESC) as steps_rank,
  -- Classify user fitness level
  CASE
    WHEN AVG(num_steps) >= 10000 THEN 'Highly Active'
    WHEN AVG(num_steps) >= 7500 THEN 'Active'
    WHEN AVG(num_steps) >= 5000 THEN 'Moderately Active'
    ELSE 'Low Activity'
  END as fitness_category
FROM LIVE.fitness_devices_silver
GROUP BY user_id;

-- Device reliability analysis
CREATE OR REFRESH LIVE TABLE device_performance
COMMENT "Analysis of device reporting patterns"
AS SELECT
  device_id,
  user_id,
  COUNT(*) as total_readings,
  COUNT(DISTINCT activity_date) as days_active,
  MIN(activity_timestamp) as first_seen,
  MAX(activity_timestamp) as last_seen,
  -- Calculate average readings per day
  COUNT(*) / NULLIF(COUNT(DISTINCT activity_date), 0) as avg_readings_per_day
FROM LIVE.fitness_devices_silver
GROUP BY device_id, user_id;
That's it for the DLT Pipeline notebook! Save this notebook now.

Step 4: Run Your Pipeline
Attach Your Notebook
Go back to your pipeline settings
Under "Source code", click "Browse"
Select your IoT_Fitness_DLT_Pipeline notebook
Click "Save"
Start the Pipeline
Click "Start" to run your pipeline
Watch the visual graph showing your data flow
Each table appears as a box with arrows showing connections
Monitor Progress
Bronze table loads first (raw data)
Then Silver (cleaned data)
Finally Gold tables (summaries)
Green checkmarks mean success!
Step 5: Explore Your Results
Create an Analysis Notebook
Create a NEW notebook called IoT_Fitness_Analysis
This is where you'll write queries to explore the data
Sample Queries to Try:
sql
-- See top 10 most active users
SELECT * FROM fitness_analytics.user_fitness_rankings
ORDER BY lifetime_steps DESC
LIMIT 10;

-- Check today's activity for all users
SELECT * FROM fitness_analytics.daily_user_summary
WHERE activity_date = current_date()
ORDER BY total_steps DESC;

-- Find the busiest hours of the day
SELECT 
  activity_hour,
  time_period,
  active_users,
  avg_steps,
  total_steps
FROM fitness_analytics.hourly_activity_patterns
ORDER BY total_steps DESC;

-- See which users achieved their daily goal
SELECT 
  user_id,
  activity_date,
  total_steps,
  daily_goal_status
FROM fitness_analytics.daily_user_summary
WHERE daily_goal_status = 'Goal Achieved'
ORDER BY activity_date DESC
LIMIT 20;

-- Analyze device reliability
SELECT * FROM fitness_analytics.device_performance
WHERE avg_readings_per_day < 10
ORDER BY last_seen DESC;
Understanding the Data Flow
Here's what happens in your pipeline:

Bronze Layer (Raw Data)
Reads JSON files from the IoT dataset
Stores everything exactly as received
Adds metadata about when/where data came from
Silver Layer (Clean Data)
Removes bad data (negative steps, missing IDs)
Converts timestamps to proper dates
Adds useful calculated fields
Categorizes activity levels
Gold Layer (Analytics)
Daily Summaries: How much each user exercised per day
Hourly Patterns: When people are most active
Rankings: Who's the most active overall
Device Analysis: Which devices report consistently
Common Issues and Solutions
"UNRESOLVED_COLUMN" error:

Check that column names match exactly
Run DESCRIBE LIVE.fitness_devices_bronze in your analysis notebook to see actual columns
"No data in tables":

Ensure the data path is correct: /databricks-datasets/iot-stream/data-device/
Check that sample datasets are available in your workspace
"DLT pipeline failed":

Look for red X marks in the pipeline graph
Click on failed tables to see error messages
Most common: typos in SQL or wrong column names
Pipeline runs but tables are empty:

The sample data might be in a different location
Try listing files: %fs ls /databricks-datasets/iot-stream/data-device/
Making It Real-Time
To simulate continuous streaming:

Change to Continuous Mode
Edit pipeline settings
Change "Pipeline mode" to "Continuous"
Pipeline will now run constantly
What Happens
Pipeline checks for new files every few seconds
Processes new data immediately
All tables update automatically
Next Steps to Try
Add More Analytics
Weekly summaries
User activity trends over time
Device battery patterns (if in data)
Create Alerts
Flag inactive users (no steps for 3 days)
Detect unusual activity spikes
Monitor device failures
Build Dashboards
Use Databricks SQL to create visualizations
Show real-time fitness metrics
Track goal achievement rates
Key Takeaways
Streaming = Processing data as it arrives, not all at once
Bronze/Silver/Gold = Industry standard for organizing data (Raw → Clean → Analytics)
DLT = Handles all the complex stuff so you can focus on your logic
Two Notebooks = One for pipeline (CREATE), one for analysis (SELECT)
Congratulations! You've built a streaming pipeline that could handle data from thousands of fitness devices in real-time!

